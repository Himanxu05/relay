import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import { Queue } from "../src/queue.js";
import { Worker } from "../src/worker.js";

const redis = new Redis(process.env.REDIS_URL!);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => Promise<boolean>, ms = 5000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await check()) return;
    await sleep(20);
  }
  throw new Error("timed out");
};
const fresh = (opts = {}) => new Queue(`wtest-${Math.random().toString(36).slice(2)}`, redis, { backoffBaseMs: 10, ...opts });

afterAll(async () => {
  for (const key of await redis.keys("relay:{wtest-*")) await redis.del(key);
  redis.disconnect();
});

describe("Worker", () => {
  it("processes jobs concurrently", async () => {
    const queue = fresh();
    let running = 0;
    let peak = 0;
    const worker = new Worker(
      queue,
      async () => {
        running++;
        peak = Math.max(peak, running);
        await sleep(30);
        running--;
      },
      { concurrency: 5 },
    );
    for (let i = 0; i < 20; i++) await queue.enqueue(i);
    worker.start();
    await until(async () => (await queue.stats()).completed === 20);
    await worker.stop();
    expect(peak).toBe(5);
  });

  it("retries failures until the handler succeeds", async () => {
    const queue = fresh();
    let calls = 0;
    const worker = new Worker(queue, async () => {
      if (++calls < 3) throw new Error("flaky");
    });
    await queue.enqueue("x", { maxAttempts: 5 });
    worker.start();
    await until(async () => (await queue.stats()).completed === 1);
    await worker.stop();
    expect(calls).toBe(3);
    expect(await queue.stats()).toMatchObject({ retried: 2, dead: 0 });
  });

  it("heartbeats keep a long job's lease alive", async () => {
    const queue = fresh({ leaseMs: 300 });
    const other = new Queue(queue.name, redis, { leaseMs: 300 });
    const worker = new Worker(queue, () => sleep(1000), { concurrency: 1 });
    await queue.enqueue("slow");
    worker.start();
    await sleep(700); // > 2 lease periods
    expect(await other.reap()).toBe(0);
    await until(async () => (await queue.stats()).completed === 1);
    await worker.stop();
  });

  it("finishes in-flight jobs on stop()", async () => {
    const queue = fresh();
    let finished = false;
    const worker = new Worker(queue, async () => {
      await sleep(200);
      finished = true;
    });
    await queue.enqueue("x");
    worker.start();
    await until(async () => (await queue.stats()).active === 1);
    await worker.stop();
    expect(finished).toBe(true);
    expect((await queue.stats()).completed).toBe(1);
  });
});
