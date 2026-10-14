import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    env: { REDIS_URL: process.env.REDIS_URL ?? "redis://127.0.0.1:6390" },
  },
});
