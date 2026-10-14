import { Redis } from "ioredis";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Queue } from "../src/queue.js";

const redis = new Redis(process.env.REDIS_URL!);
let now = 1_000_000;
const clock = () => now;
let queue: Queue;

beforeEach(async () => {
  now = 1_000_000;
  queue = new Queue(`test-${Math.random().toString(36).slice(2)}`, redis, { leaseMs: 1000, backoffBaseMs: 100, clock });
});

afterAll(async () => {
  for (const key of await redis.keys("relay:{test-*")) await redis.del(key);
  redis.disconnect();
});

describe("Queue", () => {
  it("delivers jobs in FIFO order and completes them", async () => {
    await queue.enqueue({ n: 1 });
    await queue.enqueue({ n: 2 });
    const a = await queue.claim<{ n: number }>();
    const b = await queue.claim<{ n: number }>();
    expect([a?.data.n, b?.data.n]).toEqual([1, 2]);
    expect(await queue.claim()).toBeNull();
    expect(await queue.complete(a!, { ok: true })).toBe(true);
    const info = await queue.getJob(a!.id);
    expect(info).toMatchObject({ state: "completed", attempts: 1, result: { ok: true } });
    expect(await queue.stats()).toMatchObject({ waiting: 0, active: 1, completed: 1 });
  });

  it("retries a failed job after a backoff delay, then dead-letters it", async () => {
    const { id } = await queue.enqueue("x", { maxAttempts: 2 });
    const first = await queue.claim();
    expect(await queue.fail(first!, "boom")).toBe("retrying");
    expect((await queue.getJob(id))?.state).toBe("delayed");

    now += 100; // backoff for attempt 1 is at most 100 ms
    const second = await queue.claim();
    expect(second?.attempt).toBe(2);
    expect(await queue.fail(second!, "boom again")).toBe("dead");

    expect(await queue.stats()).toMatchObject({ dead: 1, failed: 1, retried: 1, active: 0 });
    const [dead] = await queue.deadJobs();
    expect(dead).toMatchObject({ id, state: "dead", lastError: "boom again", attempts: 2 });
  });

  it("does not run delayed jobs early", async () => {
    await queue.enqueue("later", { delayMs: 5000 });
    expect(await queue.claim()).toBeNull();
    now += 4999;
    expect(await queue.claim()).toBeNull();
    now += 1;
    expect((await queue.claim())?.data).toBe("later");
  });

  it("requeues a job whose lease expired and fences off the old worker", async () => {
    await queue.enqueue("work");
    const stale = await queue.claim();
    now += 1001; // lease expired, the worker is presumed dead
    expect(await queue.reap()).toBe(1);

    const fresh = await queue.claim();
    expect(fresh?.id).toBe(stale!.id);
    expect(fresh?.attempt).toBe(2);
    // The original worker wakes up and tries to finish: rejected.
    expect(await queue.complete(stale!)).toBe(false);
    expect(await queue.fail(stale!, "late")).toBe("lease_lost");
    expect(await queue.complete(fresh!)).toBe(true);
  });

  it("keeps a job when the worker heartbeats", async () => {
    await queue.enqueue("long");
    const job = await queue.claim();
    now += 800;
    expect(await queue.extend(job!)).toBe(true);
    now += 800; // 1600 ms after claim, but only 800 since the heartbeat
    expect(await queue.reap()).toBe(0);
    expect(await queue.complete(job!)).toBe(true);
  });

  it("dead-letters a job that keeps crashing workers", async () => {
    await queue.enqueue("poison", { maxAttempts: 1 });
    await queue.claim();
    now += 1001;
    await queue.reap();
    expect(await queue.stats()).toMatchObject({ dead: 1, waiting: 0 });
  });

  it("deduplicates by idempotency key", async () => {
    const a = await queue.enqueue({ order: 42 }, { idempotencyKey: "order-42" });
    const b = await queue.enqueue({ order: 42 }, { idempotencyKey: "order-42" });
    expect(a.created).toBe(true);
    expect(b).toEqual({ id: a.id, created: false });
    expect((await queue.stats()).waiting).toBe(1);
  });

  it("lets a dead job be retried from the dashboard", async () => {
    const { id } = await queue.enqueue("x", { maxAttempts: 1 });
    await queue.fail((await queue.claim())!, "nope");
    expect(await queue.retryDead(id)).toBe(true);
    expect(await queue.retryDead(id)).toBe(false);
    expect((await queue.claim())?.attempt).toBe(1);
  });

  it("never hands the same job to two concurrent claimers", async () => {
    await Promise.all(Array.from({ length: 200 }, (_, i) => queue.enqueue(i)));
    const claims = await Promise.all(Array.from({ length: 250 }, () => queue.claim()));
    const ids = claims.filter((c) => c !== null).map((c) => c!.id);
    expect(ids).toHaveLength(200);
    expect(new Set(ids).size).toBe(200);
  });

  it("caps exponential backoff and keeps it within the jitter range", () => {
    const q = new Queue("b", redis, { backoffBaseMs: 100, backoffMaxMs: 1000 });
    for (let i = 0; i < 50; i++) {
      expect(q.backoff(1)).toBeLessThanOrEqual(100);
      expect(q.backoff(3)).toBeLessThanOrEqual(400);
      expect(q.backoff(20)).toBeLessThanOrEqual(1000);
    }
  });
});
