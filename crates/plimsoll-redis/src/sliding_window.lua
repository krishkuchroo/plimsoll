-- plimsoll sliding window log.
--
-- KEYS[1] = {plimsoll:<rule>:<key>}:ts    deque of entry timestamps (us), oldest first
-- KEYS[2] = {plimsoll:<rule>:<key>}:cost  deque of entry costs, in lockstep with ts
-- KEYS[3] = {plimsoll:<rule>:<key>}:sum   running total of costs inside the window
-- ARGV[1] = limit      max total cost per window
-- ARGV[2] = window_us  window length in microseconds
-- ARGV[3] = cost       cost of this request
-- ARGV[4] = now_us     optional test clock. When present it replaces Redis
--                      TIME and keys get no expiry (real-time TTLs would
--                      fire mid-test). Only sent by the `test-clock` feature.
--
-- Returns {allowed, used, retry_after_us, reset_after_us}.
--
-- Semantics must match plimsoll_core::WindowLog::check exactly. Timestamps
-- are formatted with %.0f because tostring() loses precision on large
-- numbers in Lua 5.1.

local ts_key, cost_key, sum_key = KEYS[1], KEYS[2], KEYS[3]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

-- 1. Clock: Redis TIME (or the test clock), clamped so it never runs
--    behind the newest entry.
local test_clock = ARGV[4] ~= nil
local now
if test_clock then
  now = tonumber(ARGV[4])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
end
local newest = redis.call('LINDEX', ts_key, -1)
if newest and tonumber(newest) > now then
  now = tonumber(newest)
end

-- 2. Running sum. Recompute from the cost deque if the sum key is missing,
--    so a lost key degrades to a slower check instead of a wrong one.
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

-- 3. Evict expired entries from the front of both deques in lockstep.
local cutoff = now - window
while true do
  local oldest = redis.call('LINDEX', ts_key, 0)
  if not oldest or tonumber(oldest) > cutoff then
    break
  end
  redis.call('LPOP', ts_key)
  local c = redis.call('LPOP', cost_key)
  sum = sum - tonumber(c or '0')
end
if sum < 0 or redis.call('LLEN', ts_key) == 0 then
  sum = 0
end

-- 4. Decide. Denied requests are not recorded.
local allowed = 0
local retry_after = 0
if sum + cost <= limit then
  allowed = 1
  redis.call('RPUSH', ts_key, string.format('%.0f', now))
  redis.call('RPUSH', cost_key, string.format('%.0f', cost))
  sum = sum + cost
else
  -- Walk from the oldest entry until enough cost has expired for this
  -- request to fit. If it never fits, report the whole window.
  --
  -- Costs are read in batches that double in size (64, 128, 256, ...) and
  -- the walk stops as soon as enough is freed, so a typical denial (cost 1)
  -- reads one small batch, and a full walk takes O(log n) calls instead of
  -- n. Only the timestamp of the entry that frees enough is read.
  retry_after = window
  if cost <= limit then
    local need = sum + cost - limit
    local freed = 0
    local batch = 64
    local start = 0
    local found = nil
    while not found do
      local costs = redis.call('LRANGE', cost_key, start, start + batch - 1)
      if #costs == 0 then break end
      for i = 1, #costs do
        freed = freed + tonumber(costs[i])
        if freed >= need then
          found = start + i - 1
          break
        end
      end
      start = start + batch
      batch = batch * 2
    end
    if found then
      retry_after = tonumber(redis.call('LINDEX', ts_key, found)) + window - now
      if retry_after < 0 then retry_after = 0 end
    end
  end
end

-- 5. Persist. Every key expires one window after the newest entry.
local reset_after = 0
local oldest = redis.call('LINDEX', ts_key, 0)
if oldest then
  reset_after = tonumber(oldest) + window - now
  if reset_after < 0 then reset_after = 0 end
  if test_clock then
    redis.call('SET', sum_key, string.format('%.0f', sum))
  else
    local ttl_ms = math.ceil(window / 1000)
    redis.call('SET', sum_key, string.format('%.0f', sum), 'PX', ttl_ms)
    redis.call('PEXPIRE', ts_key, ttl_ms)
    redis.call('PEXPIRE', cost_key, ttl_ms)
  end
else
  redis.call('DEL', ts_key, cost_key, sum_key)
end

return {
  allowed,
  string.format('%.0f', sum),
  string.format('%.0f', retry_after),
  string.format('%.0f', reset_after),
}
