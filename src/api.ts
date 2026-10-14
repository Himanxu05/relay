import express from "express";
import { Redis } from "ioredis";
import { fileURLToPath } from "node:url";
import { z, ZodError } from "zod";
import { Queue } from "./queue.js";

const enqueueBody = z.object({
  data: z.unknown(),
  maxAttempts: z.number().int().min(1).max(25).optional(),
  delayMs: z.number().int().min(0).max(7 * 24 * 3600 * 1000).optional(),
  idempotencyKey: z.string().min(1).max(200).optional(),
});
const queueName = z.string().regex(/^[a-zA-Z0-9_-]{1,64}$/);

export function createApp(redis: Redis) {
  const app = express();
  const queues = new Map<string, Queue>();
  const queue = (name: string) => {
    const valid = queueName.parse(name);
    if (!queues.has(valid)) queues.set(valid, new Queue(valid, redis));
    return queues.get(valid)!;
  };

  app.use(express.json({ limit: "256kb" }));
  app.use(express.static(fileURLToPath(new URL("../public", import.meta.url))));

  app.get("/health", async (_req, res) => {
    try {
      await redis.ping();
      res.json({ status: "ok" });
    } catch {
      res.status(503).json({ status: "redis_unavailable" });
    }
  });

  app.post("/queues/:queue/jobs", async (req, res) => {
    const { data, ...options } = enqueueBody.parse(req.body);
    const { id, created } = await queue(req.params.queue).enqueue(data, options);
    // 201 for a new job, 200 when the idempotency key matched an existing one.
    res.status(created ? 201 : 200).json({ id, created });
  });

  app.get("/queues/:queue/jobs/:id", async (req, res) => {
    const job = await queue(req.params.queue).getJob(req.params.id);
    if (!job) {
      res.status(404).json({ error: "Job not found" });
      return;
    }
    res.json(job);
  });

  app.get("/queues/:queue/stats", async (req, res) => {
    res.json(await queue(req.params.queue).stats());
  });

  app.get("/queues/:queue/dead", async (req, res) => {
    const limit = z.coerce.number().int().min(1).max(500).default(50).parse(req.query.limit);
    res.json(await queue(req.params.queue).deadJobs(limit));
  });

  app.post("/queues/:queue/dead/:id/retry", async (req, res) => {
    const ok = await queue(req.params.queue).retryDead(req.params.id);
    res.status(ok ? 200 : 404).json(ok ? { retried: true } : { error: "Not in the dead-letter queue" });
  });

  app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    if (err instanceof ZodError) {
      res.status(400).json({ error: "Invalid request", details: err.issues });
      return;
    }
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });
  return app;
}
