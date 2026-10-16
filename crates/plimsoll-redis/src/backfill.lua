-- plimsoll backfill: record requests admitted by the in-process fallback
-- while Redis was unreachable, so the shared window counts them.
--
-- KEYS[1..3] = ts, cost, sum keys (same as sliding_window.lua)
-- ARGV[1]    = window_us
-- ARGV[2..]  = pairs of at_unix_us, cost, in admission order
--
-- Entries already outside the window are skipped. Each entry's time is
-- clamped to at most Redis TIME (a client clock running ahead can't push it
-- into the future) and to at least the newest entry (the log stays ordered;
-- recording an entry late only makes the limit stricter). Returns the
-- number of entries recorded.

local ts_key, cost_key, sum_key = KEYS[1], KEYS[2], KEYS[3]
local window = tonumber(ARGV[1])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local sum
local raw_sum = redis.call('GET', sum_key)
if raw_sum then
  sum = tonumber(raw_sum)
else
  sum = 0
  for _, c in ipairs(redis.call('LRANGE', cost_key, 0, -1)) do
    sum = sum + tonumber(c)
  end
end

local newest = tonumber(redis.call('LINDEX', ts_key, -1) or '0')
local recorded = 0
for i = 2, #ARGV, 2 do
  local at = tonumber(ARGV[i])
  local cost = tonumber(ARGV[i + 1])
  if at > now - window then
    if at > now then at = now end
    if at < newest then at = newest end
    redis.call('RPUSH', ts_key, string.format('%.0f', at))
    redis.call('RPUSH', cost_key, string.format('%.0f', cost))
    sum = sum + cost
    newest = at
    recorded = recorded + 1
  end
end

if recorded > 0 then
  local ttl_ms = math.ceil(window / 1000)
  redis.call('SET', sum_key, string.format('%.0f', sum), 'PX', ttl_ms)
  redis.call('PEXPIRE', ts_key, ttl_ms)
  redis.call('PEXPIRE', cost_key, ttl_ms)
end
return recorded
