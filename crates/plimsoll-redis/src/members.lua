-- plimsoll instance membership.
--
-- KEYS[1] = {<prefix>}:instances  sorted set: member = instance id, score = expiry (ms)
-- ARGV[1] = id       this instance's id
-- ARGV[2] = ttl_ms   how long this registration lives without a heartbeat
-- ARGV[3] = leave    "1" to remove this instance instead of refreshing it
--
-- Returns the ids of all live instances (any order; the caller sorts).
-- Time comes from Redis TIME so instances with skewed clocks still agree.

local key, id = KEYS[1], ARGV[1]
local ttl = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

if ARGV[3] == '1' then
  redis.call('ZREM', key, id)
else
  redis.call('ZADD', key, string.format('%.0f', now + ttl), id)
end
redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. string.format('%.0f', now))
redis.call('PEXPIRE', key, ttl * 2)
return redis.call('ZRANGE', key, 0, -1)
