import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { CLAIM, COMPLETE, ENQUEUE, EXTEND, FAIL, REAP, RETRY_DEAD } from "./scripts.js";

export interface EnqueueOptions {
  maxAttempts?: number;
  delayMs?: number;
  idempotencyKey?: string;
}

export interface ClaimedJob<T = unknown> {
  id: string;
  data: T;
  attempt: number;
  maxAttempts: number;
  token: string;
}

export interface JobInfo {
  id: string;
  state: "waiting" | "delayed" | "active" | "completed" | "dead";
  data: unknown;
  attempts: number;
  maxAttempts: number;
  lastError?: string;
  result?: unknown;
  createdAt: number;
  startedAt?: number;
  finishedAt?: number;
}

export interface QueueOptions {
  leaseMs?: number; // how long a claim lasts without a heartbeat
  backoffBaseMs?: number; // first retry delay; doubles each attempt
  backoffMaxMs?: number;
  completedTtlSec?: number; // keep finished jobs this long for inspection
  clock?: () => number; // injectable for tests
}

type Scripted = Redis & Record<"relayEnqueue" | "relayClaim" | "relayComplete" | "relayFail" | "relayExtend" | "relayReap" | "relayRetryDead", (...args: (string | number)[]) => Promise<unknown>>;

export class Queue {
  readonly redis: Scripted;
  readonly leaseMs: number;
  private readonly prefix: string;
  private readonly opts: Required<Omit<QueueOptions, "leaseMs">>;

  constructor(
    readonly name: string,
    redis: Redis,
    options: QueueOptions = {},
  ) {
    this.prefix = `relay:{${name}}:`;
    this.leaseMs = options.leaseMs ?? 30_000;
    this.opts = {
      backoffBaseMs: options.backoffBaseMs ?? 1_000,
      backoffMaxMs: options.backoffMaxMs ?? 5 * 60_000,
      completedTtlSec: options.completedTtlSec ?? 24 * 3600,
      clock: options.clock ?? Date.now,
    };
    // defineCommand loads each script once and calls it by SHA (EVALSHA).
    const r = redis as Scripted;
    const define = (name: string, numberOfKeys: number, lua: string) => {
      if (!(name in r)) r.defineCommand(name, { numberOfKeys, lua });
    };
    define("relayEnqueue", 4, ENQUEUE);
    define("relayClaim", 3, CLAIM);
    define("relayComplete", 3, COMPLETE);
    define("relayFail", 5, FAIL);
    define("relayExtend", 2, EXTEND);
    define("relayReap", 4, REAP);
    define("relayRetryDead", 3, RETRY_DEAD);
    this.redis = r;
  }

  private key(k: string) {
    return this.prefix + k;
  }

  private now() {
    return this.opts.clock();
  }

  async enqueue(data: unknown, options: EnqueueOptions = {}): Promise<{ id: string; created: boolean }> {
    const id = randomUUID();
    const now = this.now();
    const [jobId, created] = (await this.redis.relayEnqueue(
      this.key("waiting"),
      this.key("delayed"),
      this.key(`job:${id}`),
      options.idempotencyKey ? this.key(`idem:${options.idempotencyKey}`) : "",
      id,
      JSON.stringify(data ?? null),
      options.maxAttempts ?? 3,
      now + (options.delayMs ?? 0),
      now,
      24 * 3600,
    )) as [string, number];
    return { id: jobId, created: created === 1 };
  }

  async claim<T = unknown>(): Promise<ClaimedJob<T> | null> {
    const token = randomUUID();
    const res = (await this.redis.relayClaim(
      this.key("waiting"),
      this.key("delayed"),
      this.key("active"),
      this.now(),
      this.leaseMs,
      token,
      this.prefix,
    )) as [string, string, number, string] | null;
    if (!res) return null;
    const [id, data, attempt, maxAttempts] = res;
    return { id, data: JSON.parse(data) as T, attempt, maxAttempts: Number(maxAttempts), token };
  }

  // Returns false if the lease was lost (expired and reaped): the result is
  // discarded because another worker may already be running the job.
  async complete(job: ClaimedJob, result: unknown = null): Promise<boolean> {
    const ok = await this.redis.relayComplete(
      this.key("active"),
      this.key(`job:${job.id}`),
      this.key("stats"),
      job.id,
      job.token,
      this.now(),
      JSON.stringify(result ?? null),
      this.opts.completedTtlSec,
    );
    return ok === 1;
  }

  async fail(job: ClaimedJob, error: string): Promise<"retrying" | "dead" | "lease_lost"> {
    const res = await this.redis.relayFail(
      this.key("active"),
      this.key("delayed"),
      this.key("dead"),
      this.key(`job:${job.id}`),
      this.key("stats"),
      job.id,
      job.token,
      this.now(),
      error.slice(0, 1000),
      this.backoff(job.attempt),
    );
    return (res as "retrying" | "dead" | null) ?? "lease_lost";
  }

  // Exponential backoff with "full jitter": a random delay between 0 and
  // base * 2^(attempt-1), capped. Jitter spreads retries out so that many
  // jobs failing together (e.g. a downstream outage) don't all retry at once.
  backoff(attempt: number): number {
    const ceiling = Math.min(this.opts.backoffMaxMs, this.opts.backoffBaseMs * 2 ** (attempt - 1));
    return Math.round(Math.random() * ceiling);
  }

  async extend(job: ClaimedJob): Promise<boolean> {
    const ok = await this.redis.relayExtend(
      this.key("active"),
      this.key(`job:${job.id}`),
      job.id,
      job.token,
      this.now() + this.leaseMs,
    );
    return ok === 1;
  }

  async reap(): Promise<number> {
    return (await this.redis.relayReap(
      this.key("active"),
      this.key("waiting"),
      this.key("dead"),
      this.key("stats"),
      this.now(),
      this.prefix,
      "lease expired: worker crashed or stalled",
    )) as number;
  }

  async retryDead(id: string): Promise<boolean> {
    return (await this.redis.relayRetryDead(this.key("dead"), this.key("waiting"), this.key(`job:${id}`), id)) === 1;
  }

  async getJob(id: string): Promise<JobInfo | null> {
    const h = await this.redis.hgetall(this.key(`job:${id}`));
    if (!h.id) return null;
    const num = (v?: string) => (v === undefined ? undefined : Number(v));
    return {
      id: h.id,
      state: h.state as JobInfo["state"],
      data: JSON.parse(h.data ?? "null"),
      attempts: Number(h.attempts),
      maxAttempts: Number(h.maxAttempts),
      lastError: h.lastError,
      result: h.result === undefined ? undefined : JSON.parse(h.result),
      createdAt: Number(h.createdAt),
      startedAt: num(h.startedAt),
      finishedAt: num(h.finishedAt),
    };
  }

  async stats() {
    const [waiting, delayed, active, dead, counters] = await this.redis
      .multi()
      .llen(this.key("waiting"))
      .zcard(this.key("delayed"))
      .zcard(this.key("active"))
      .llen(this.key("dead"))
      .hgetall(this.key("stats"))
      .exec()
      .then((rows) => rows!.map(([err, v]) => {
        if (err) throw err;
        return v;
      }));
    const c = counters as Record<string, string>;
    return {
      waiting: waiting as number,
      delayed: delayed as number,
      active: active as number,
      dead: dead as number,
      completed: Number(c.completed ?? 0),
      failed: Number(c.failed ?? 0),
      retried: Number(c.retried ?? 0),
    };
  }

  async deadJobs(limit = 50): Promise<JobInfo[]> {
    const ids = await this.redis.lrange(this.key("dead"), 0, limit - 1);
    const jobs = await Promise.all(ids.map((id) => this.getJob(id)));
    return jobs.filter((j): j is JobInfo => j !== null);
  }

  // Delete every key of this queue (tests and benchmarks).
  async obliterate(): Promise<void> {
    let cursor = "0";
    do {
      const [next, keys] = await this.redis.scan(cursor, "MATCH", `${this.prefix}*`, "COUNT", 1000);
      if (keys.length) await this.redis.del(...keys);
      cursor = next;
    } while (cursor !== "0");
  }
}
