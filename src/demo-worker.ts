// Demo worker for the "emails" queue: pretends to send an email, takes
// 20–200 ms, and fails 20% of the time so retries and the dead-letter queue
// have something to show.
import { Redis } from "ioredis";
import { Queue } from "./queue.js";
import { Worker } from "./worker.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const queue = new Queue("emails", redis, { leaseMs: 10_000, backoffBaseMs: 500 });
const failRate = Number(process.env.FAIL_RATE ?? 0.2);

const worker = new Worker<{ to: string }>(
  queue,
  async (data) => {
    await new Promise((r) => setTimeout(r, 20 + Math.random() * 180));
    if (Math.random() < failRate) throw new Error(`SMTP timeout sending to ${data.to}`);
    return { delivered: true };
  },
  {
    concurrency: Number(process.env.CONCURRENCY ?? 8),
    onEvent: (event, job, detail) => {
      if (event !== "completed") console.log(`${event} ${job.id} attempt ${job.attempt}/${job.maxAttempts} ${detail ?? ""}`);
    },
  },
);

worker.start();
console.log(`worker ${process.pid} started`);

// Docker sends SIGTERM on stop: finish in-flight jobs, then exit.
process.on("SIGTERM", async () => {
  console.log("SIGTERM: finishing in-flight jobs");
  await worker.stop();
  redis.disconnect();
  process.exit(0);
});
