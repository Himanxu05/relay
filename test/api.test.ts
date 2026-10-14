import { Redis } from "ioredis";
import request from "supertest";
import { afterAll, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { Queue } from "../src/queue.js";

const redis = new Redis(process.env.REDIS_URL!);
const app = createApp(redis);
const q = `atest-${Math.random().toString(36).slice(2)}`;

afterAll(async () => {
  for (const key of await redis.keys(`relay:{${q}}*`)) await redis.del(key);
  redis.disconnect();
});

describe("HTTP API", () => {
  it("enqueues, reports stats and fetches a job", async () => {
    const created = await request(app).post(`/queues/${q}/jobs`).send({ data: { to: "a@b.c" } });
    expect(created.status).toBe(201);
    const job = await request(app).get(`/queues/${q}/jobs/${created.body.id}`);
    expect(job.body).toMatchObject({ state: "waiting", data: { to: "a@b.c" }, maxAttempts: 3 });
    const stats = await request(app).get(`/queues/${q}/stats`);
    expect(stats.body.waiting).toBe(1);
  });

  it("returns 200 with the same id for a repeated idempotency key", async () => {
    const body = { data: 1, idempotencyKey: "k1" };
    const a = await request(app).post(`/queues/${q}/jobs`).send(body);
    const b = await request(app).post(`/queues/${q}/jobs`).send(body);
    expect([a.status, b.status]).toEqual([201, 200]);
    expect(b.body.id).toBe(a.body.id);
  });

  it("validates input", async () => {
    expect((await request(app).post(`/queues/${q}/jobs`).send({ data: 1, maxAttempts: 0 })).status).toBe(400);
    expect((await request(app).get("/queues/bad name!/stats")).status).toBe(400);
    expect((await request(app).get(`/queues/${q}/jobs/nope`)).status).toBe(404);
  });

  it("lists and retries dead jobs", async () => {
    const queue = new Queue(q, redis);
    const { id } = await queue.enqueue("x", { maxAttempts: 1 });
    // drain the queue until we get our job, then fail it
    for (;;) {
      const job = await queue.claim();
      if (job!.id === id) {
        await queue.fail(job!, "broken");
        break;
      }
      await queue.complete(job!);
    }
    const dead = await request(app).get(`/queues/${q}/dead`);
    expect(dead.body.map((j: { id: string }) => j.id)).toContain(id);
    expect((await request(app).post(`/queues/${q}/dead/${id}/retry`)).status).toBe(200);
    expect((await request(app).post(`/queues/${q}/dead/${id}/retry`)).status).toBe(404);
  });
});
