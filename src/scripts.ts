// Lua scripts. Redis runs each script atomically (no other command runs in
// between), which is what makes moving a job between lists race-free even
// with many workers.
//
// Keys for a queue named Q (the {Q} hash tag keeps them in one Redis Cluster slot):
//   relay:{Q}:waiting   LIST   job ids ready to run (LPUSH in, RPOP out = FIFO)
//   relay:{Q}:delayed   ZSET   job ids scored by when they may run (retries, delayed jobs)
//   relay:{Q}:active    ZSET   job ids scored by lease expiry
//   relay:{Q}:dead      LIST   job ids that used up their attempts
//   relay:{Q}:job:<id>  HASH   the job
//   relay:{Q}:stats     HASH   completed / failed / retried counters
//   relay:{Q}:idem:<k>  STRING idempotency key -> job id

// KEYS: waiting, delayed, jobKey, idemKey ("" if none)
// ARGV: id, data, maxAttempts, runAt, now, idemTtlSec
export const ENQUEUE = `
if KEYS[4] ~= "" then
  local existing = redis.call("GET", KEYS[4])
  if existing then return {existing, 0} end
  redis.call("SET", KEYS[4], ARGV[1], "EX", ARGV[6])
end
redis.call("HSET", KEYS[3], "id", ARGV[1], "data", ARGV[2], "attempts", 0,
  "maxAttempts", ARGV[3], "state", "waiting", "createdAt", ARGV[5])
if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
  redis.call("HSET", KEYS[3], "state", "delayed")
  redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
else
  redis.call("LPUSH", KEYS[1], ARGV[1])
end
return {ARGV[1], 1}
`;

// Move due delayed jobs to waiting, then claim the oldest waiting job with a
// lease. The token identifies this claim; complete/fail must present it.
// KEYS: waiting, delayed, active   ARGV: now, leaseMs, token, prefix
export const CLAIM = `
local due = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(due) do
  redis.call("ZREM", KEYS[2], id)
  redis.call("LPUSH", KEYS[1], id)
  redis.call("HSET", ARGV[4] .. "job:" .. id, "state", "waiting")
end
local id = redis.call("RPOP", KEYS[1])
if not id then return false end
local job = ARGV[4] .. "job:" .. id
redis.call("ZADD", KEYS[3], tonumber(ARGV[1]) + tonumber(ARGV[2]), id)
local attempts = redis.call("HINCRBY", job, "attempts", 1)
redis.call("HSET", job, "state", "active", "token", ARGV[3], "startedAt", ARGV[1])
return {id, redis.call("HGET", job, "data"), attempts, redis.call("HGET", job, "maxAttempts")}
`;

// KEYS: active, jobKey, stats   ARGV: id, token, now, result, ttlSec
export const COMPLETE = `
if redis.call("HGET", KEYS[2], "token") ~= ARGV[2] then return 0 end
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HSET", KEYS[2], "state", "completed", "finishedAt", ARGV[3], "result", ARGV[4])
redis.call("HDEL", KEYS[2], "token")
redis.call("EXPIRE", KEYS[2], ARGV[5])
redis.call("HINCRBY", KEYS[3], "completed", 1)
return 1
`;

// Retry with a delay, or move to the dead-letter list after the last attempt.
// The retry delay is computed by the caller (exponential backoff + jitter),
// because Lua's random() inside Redis is deterministic.
// KEYS: active, delayed, dead, jobKey, stats   ARGV: id, token, now, error, retryDelayMs
export const FAIL = `
if redis.call("HGET", KEYS[4], "token") ~= ARGV[2] then return false end
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[4], "token")
redis.call("HSET", KEYS[4], "lastError", ARGV[4])
local attempts = tonumber(redis.call("HGET", KEYS[4], "attempts"))
local maxAttempts = tonumber(redis.call("HGET", KEYS[4], "maxAttempts"))
if attempts < maxAttempts then
  redis.call("HSET", KEYS[4], "state", "delayed")
  redis.call("ZADD", KEYS[2], tonumber(ARGV[3]) + tonumber(ARGV[5]), ARGV[1])
  redis.call("HINCRBY", KEYS[5], "retried", 1)
  return "retrying"
end
redis.call("HSET", KEYS[4], "state", "dead", "finishedAt", ARGV[3])
redis.call("LPUSH", KEYS[3], ARGV[1])
redis.call("HINCRBY", KEYS[5], "failed", 1)
return "dead"
`;

// Heartbeat: push the lease forward, only if this worker still holds it.
// KEYS: active, jobKey   ARGV: id, token, newExpiry
export const EXTEND = `
if redis.call("HGET", KEYS[2], "token") ~= ARGV[2] then return 0 end
redis.call("ZADD", KEYS[1], "XX", ARGV[3], ARGV[1])
return 1
`;

// Recover jobs whose lease expired (the worker crashed or hung). Removing the
// token "fences off" the old worker: if it wakes up later, its complete/fail
// is rejected. The job counts the lost run as an attempt.
// KEYS: active, waiting, dead, stats   ARGV: now, prefix, error
export const REAP = `
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(expired) do
  local job = ARGV[2] .. "job:" .. id
  redis.call("ZREM", KEYS[1], id)
  redis.call("HDEL", job, "token")
  redis.call("HSET", job, "lastError", ARGV[3])
  if tonumber(redis.call("HGET", job, "attempts")) < tonumber(redis.call("HGET", job, "maxAttempts")) then
    redis.call("HSET", job, "state", "waiting")
    redis.call("RPUSH", KEYS[2], id) -- RPUSH = front of the line: it already waited once
    redis.call("HINCRBY", KEYS[4], "retried", 1)
  else
    redis.call("HSET", job, "state", "dead", "finishedAt", ARGV[1])
    redis.call("LPUSH", KEYS[3], id)
    redis.call("HINCRBY", KEYS[4], "failed", 1)
  end
end
return #expired
`;

// KEYS: dead, waiting, jobKey   ARGV: id
export const RETRY_DEAD = `
if redis.call("LREM", KEYS[1], 1, ARGV[1]) == 0 then return 0 end
redis.call("HSET", KEYS[3], "state", "waiting", "attempts", 0)
redis.call("HDEL", KEYS[3], "finishedAt")
redis.call("LPUSH", KEYS[2], ARGV[1])
return 1
`;
