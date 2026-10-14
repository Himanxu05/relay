import { Redis } from "ioredis";
import { createApp } from "./api.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const port = Number(process.env.PORT ?? 3000);
const server = createApp(redis).listen(port, () => console.log(`relay api on :${port}`));

process.on("SIGTERM", () => {
  server.close(() => {
    redis.disconnect();
    process.exit(0);
  });
});
