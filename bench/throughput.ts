// Throughput: N no-op jobs, W worker processes × C concurrency each.
//   tsx bench/throughput.ts [jobs=50000] [workers=4] [concurrency=16]
import { Redis } from "ioredis";
import { Queue } from "../src/queue.js";
import { REDIS_URL, fill, spawnWorker, waitFor } from "./common.js";

const [jobs = 50_000, workers = 4, concurrency = 16] = process.argv.slice(2).map(Number);
const redis = new Redis(REDIS_URL);
const queue = new Queue(`bench-tp-${Date.now()}`, redis);

const enqueueRate = await fill(redis, queue, jobs);
const t0 = performance.now();
const children = Array.from({ length: workers }, () =>
  spawnWorker({ QUEUE: queue.name, MODE: "noop", CONCURRENCY: String(concurrency) }),
);
await waitFor(async () => (await queue.stats()).completed === jobs, 300_000);
const seconds = (performance.now() - t0) / 1000;

children.forEach((c) => c.kill("SIGTERM"));
console.log(
  JSON.stringify({ jobs, workers, concurrency, enqueuePerSec: Math.round(enqueueRate), seconds: +seconds.toFixed(2), processedPerSec: Math.round(jobs / seconds) }),
);
await queue.obliterate();
redis.disconnect();
