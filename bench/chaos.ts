// Chaos test: run jobs while repeatedly SIGKILLing worker processes (no
// graceful shutdown, no chance to release leases), then check that every
// job still ran and count how many ran more than once.
//   tsx bench/chaos.ts [jobs=5000] [workers=4] [killEveryMs=1000]
import type { ChildProcess } from "node:child_process";
import { Redis } from "ioredis";
import { Queue } from "../src/queue.js";
import { REDIS_URL, fill, spawnWorker, waitFor } from "./common.js";

const [jobs = 5000, workers = 4, killEveryMs = 1000] = process.argv.slice(2).map(Number);
const LEASE_MS = 2000;
const redis = new Redis(REDIS_URL);
const queue = new Queue(`bench-chaos-${Date.now()}`, redis, { leaseMs: LEASE_MS });
const env = { QUEUE: queue.name, MODE: "chaos", CONCURRENCY: "8", LEASE_MS: String(LEASE_MS) };

await fill(redis, queue, jobs);
const t0 = performance.now();
const pool: ChildProcess[] = Array.from({ length: workers }, () => spawnWorker(env));
let kills = 0;

// Something has to reap expired leases even if every worker was just killed.
const reaper = setInterval(() => void queue.reap(), LEASE_MS / 2);
const killer = setInterval(() => {
  const i = Math.floor(Math.random() * pool.length);
  pool[i]!.kill("SIGKILL");
  kills++;
  pool[i] = spawnWorker(env);
}, killEveryMs);

await waitFor(async () => {
  const s = await queue.stats();
  return s.completed + s.dead === jobs && s.active === 0;
}, 600_000);
clearInterval(killer);
clearInterval(reaper);
pool.forEach((c) => c.kill("SIGTERM"));

const runs = Object.values(await redis.hgetall(`bench:{${queue.name}}:runs`)).map(Number);
const stats = await queue.stats();
console.log(
  JSON.stringify({
    jobs,
    workerKills: kills,
    seconds: +((performance.now() - t0) / 1000).toFixed(1),
    completed: stats.completed,
    dead: stats.dead,
    lost: jobs - runs.length, // jobs whose side effect never happened
    ranMoreThanOnce: runs.filter((n) => n > 1).length,
    leaseExpiriesRecovered: stats.retried,
  }),
);
await queue.obliterate();
await redis.del(`bench:{${queue.name}}:runs`);
redis.disconnect();
