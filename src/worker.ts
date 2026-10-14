import type { ClaimedJob, Queue } from "./queue.js";

export type Handler<T = unknown> = (data: T, job: ClaimedJob<T>) => Promise<unknown>;

export interface WorkerOptions {
  concurrency?: number; // jobs run in parallel by this process
  pollMinMs?: number; // idle polling starts here...
  pollMaxMs?: number; // ...and backs off up to here while the queue stays empty
  shutdownTimeoutMs?: number;
  onEvent?: (event: "completed" | "retrying" | "dead" | "lease_lost", job: ClaimedJob, detail?: string) => void;
}

// Runs `concurrency` claim loops. Each loop: claim a job, run the handler
// while sending heartbeats, then complete or fail it. A periodic reaper
// requeues jobs whose lease expired (a worker died). Every worker runs the
// reaper; that is safe because the reap script is atomic.
export class Worker<T = unknown> {
  private running = false;
  private loops: Promise<void>[] = [];
  private reaper?: NodeJS.Timeout;
  private readonly sleepers = new Set<() => void>();
  private readonly opts: Required<Omit<WorkerOptions, "onEvent">> & Pick<WorkerOptions, "onEvent">;

  constructor(
    private readonly queue: Queue,
    private readonly handler: Handler<T>,
    options: WorkerOptions = {},
  ) {
    this.opts = {
      concurrency: options.concurrency ?? 4,
      pollMinMs: options.pollMinMs ?? 10,
      pollMaxMs: options.pollMaxMs ?? 500,
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? 30_000,
      onEvent: options.onEvent,
    };
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loops = Array.from({ length: this.opts.concurrency }, () => this.loop());
    const reapEvery = Math.max(100, Math.floor(this.queue.leaseMs / 2));
    this.reaper = setInterval(() => void this.queue.reap().catch(() => undefined), reapEvery);
  }

  // Graceful shutdown: stop claiming new jobs and let in-flight jobs finish.
  // Jobs still running after the timeout are abandoned; their leases expire
  // and another worker picks them up.
  async stop(): Promise<void> {
    this.running = false;
    clearInterval(this.reaper);
    for (const wake of this.sleepers) wake();
    const timeout = new Promise((r) => setTimeout(r, this.opts.shutdownTimeoutMs).unref());
    await Promise.race([Promise.all(this.loops), timeout]);
  }

  private async loop(): Promise<void> {
    let idle = this.opts.pollMinMs;
    while (this.running) {
      let job: ClaimedJob<T> | null;
      try {
        job = await this.queue.claim<T>();
      } catch {
        job = null; // Redis blip: back off and try again
      }
      if (!job) {
        await this.sleep(idle);
        idle = Math.min(this.opts.pollMaxMs, idle * 2);
        continue;
      }
      idle = this.opts.pollMinMs;
      await this.run(job);
    }
  }

  private async run(job: ClaimedJob<T>): Promise<void> {
    try {
      await this.execute(job);
    } catch {
      // Couldn't record the outcome (Redis unreachable). Nothing to do here:
      // the lease will expire and the job will run again (at-least-once).
    }
  }

  private async execute(job: ClaimedJob<T>): Promise<void> {
    // Heartbeat at a third of the lease, so two missed beats still leave time.
    const heartbeat = setInterval(() => void this.queue.extend(job).catch(() => undefined), this.queue.leaseMs / 3);
    let result: unknown;
    try {
      result = await this.handler(job.data, job);
    } catch (err) {
      clearInterval(heartbeat);
      const message = err instanceof Error ? err.message : String(err);
      // Not inlined into onEvent?.(...): optional calls skip evaluating their
      // arguments when onEvent is undefined, so fail() would never run.
      const outcome = await this.queue.fail(job, message);
      this.opts.onEvent?.(outcome, job, message);
      return;
    }
    clearInterval(heartbeat);
    const ok = await this.queue.complete(job, result);
    this.opts.onEvent?.(ok ? "completed" : "lease_lost", job);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.sleepers.delete(done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.sleepers.add(done);
    });
  }
}
