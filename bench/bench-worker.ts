// Worker process used by the benchmarks.
//   MODE=noop   handler does nothing (measures queue overhead)
//   MODE=chaos  handler sleeps 5–30 ms, then records that it ran the job
import { Redis } from "ioredis";
import { Queue } from "../src/queue.js";
import { Worker } from "../src/worker.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const name = process.env.QUEUE!;
const queue = new Queue(name, redis, { leaseMs: Number(process.env.LEASE_MS ?? 30_000) });
const chaos = process.env.MODE === "chaos";

const worker = new Worker<number>(
  queue,
  async (_data, job) => {
    if (!chaos) return;
    await new Promise((r) => setTimeout(r, 5 + Math.random() * 25));
    // The job's side effect. If this process is killed after this line but
    // before complete(), the job runs again elsewhere: at-least-once.
    await redis.hincrby(`bench:{${name}}:runs`, job.id, 1);
  },
  { concurrency: Number(process.env.CONCURRENCY ?? 16), pollMaxMs: 50 },
);
worker.start();
process.on("SIGTERM", async () => {
  await worker.stop();
  process.exit(0);
});
