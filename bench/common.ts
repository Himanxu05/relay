import { spawn, type ChildProcess } from "node:child_process";
import { Redis } from "ioredis";
import { Queue } from "../src/queue.js";

export const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

export function spawnWorker(env: Record<string, string>): ChildProcess {
  return spawn(process.execPath, ["--import", "tsx", "bench/bench-worker.ts"], {
    env: { ...process.env, REDIS_URL, ...env },
    stdio: "inherit",
  });
}

// Enqueue in pipelined batches: one round trip per 1,000 jobs.
export async function fill(redis: Redis, queue: Queue, count: number): Promise<number> {
  const t0 = performance.now();
  for (let i = 0; i < count; i += 1000) {
    await Promise.all(Array.from({ length: Math.min(1000, count - i) }, (_, j) => queue.enqueue(i + j)));
  }
  return (count / (performance.now() - t0)) * 1000;
}

export async function waitFor(check: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("benchmark timed out");
}
