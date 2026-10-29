-- Token-bucket admission: three-layer gate.
--
-- Layer 1 (global): hard cap on aggregate throughput for the target.
-- Layer 2 (per-priority): per-priority refill buckets enforce the configured
--   priority-share fractions, a priority is soft-denied when it is overdrawn
--   and another priority still has capacity AND has been active recently
--   (cross-priority yield). Idle priorities (no recent acquire) do NOT block
--   other priorities, their full buckets are absorbed as headroom.
-- Layer 3 (per-tenant): per-(tenant, priority) token bucket at the tenant's
--   fair-share refill rate.  Active tenants are tracked incrementally in a
--   per-priority sorted set (KEYS[5]) + aggregate hash (KEYS[6]) so the
--   fair-share denominator is O(1) on the hot path, stale entries are evicted
--   so dormant tenants do not shrink the denominator.
--
-- Returns {1, granted_at_ms} on grant, {0, retry_after_ms} on denial.
-- See docs/architecture.md for the full algorithm and decision diagram.
--
-- KEYS[1] = bpq:{target}:tb:global    hash  budget, last_ms
-- KEYS[2] = bpq:{target}:tb:pri       hash  "1","2","3","last_ms",
--                                           "1_act","2_act","3_act" (last-active ms per pri)
-- KEYS[3] = bpq:{target}:tb:tp        hash  "tenant:priority" -> "bucket:last_used_ms"
-- KEYS[4] = bpq:{target}:tb:reg       hash  tenant registry + sweep timestamps
-- KEYS[5] = bpq:{target}:tb:act:{pri} zset  member=tenant, score=last_used_ms (active set)
-- KEYS[6] = bpq:{target}:tb:agg:{pri} hash  sum_w, cfg_ver, rebuild_ms (incremental aggregate)
--
-- ARGV: op, tenant_id, priority, cost, now_ms, limit_rps, burst_seconds,
--       global_burst_mult, idle_ms, share_1, share_2, share_3,
--       my_weight, tenant_weights_json, default_weight, pri_idle_ms, cfg_ver

local op        = ARGV[1]
local tenant_id = ARGV[2]
local pri_str   = ARGV[3]
local cost      = tonumber(ARGV[4])
local now_ms    = tonumber(ARGV[5])
local limit_rps = tonumber(ARGV[6])
local burst_s   = tonumber(ARGV[7])
local g_mult    = tonumber(ARGV[8])
local idle_ms   = tonumber(ARGV[9])
local share1    = tonumber(ARGV[10])
local share2    = tonumber(ARGV[11])
local share3    = tonumber(ARGV[12])
local my_w      = tonumber(ARGV[13])
local weights_json = ARGV[14] or '{}'
local default_w = tonumber(ARGV[15] or '1') or 1.0
-- Cross-priority yield only blocks when the peer priority has been active
-- within this many milliseconds. Default 2s, long enough to absorb a P1 burst
-- response slot for a P2 tenant that just paused, short enough that long-idle
-- priorities do not reserve capacity they will not use.
local pri_idle_ms = tonumber(ARGV[16] or '2000') or 2000.0
local cfg_ver   = ARGV[17] or ''

if cost == nil or cost <= 0 then
  return {1, now_ms}
end

if my_w == nil or my_w <= 0 then
  my_w = 1.0
end
if share1 == nil then share1 = 0.0 end
if share2 == nil then share2 = 0.0 end
if share3 == nil then share3 = 0.0 end

local pri = tonumber(pri_str)
if pri == nil then pri = 3 end
if pri < 1 then pri = 1 end
if pri > 3 then pri = 3 end
local pri_key = tostring(math.floor(pri))

local share_p = share1
if pri == 2 then share_p = share2 elseif pri == 3 then share_p = share3 end
if share_p == nil or share_p < 0 then share_p = 0.0 end

local weights = {}
local ok, decoded = pcall(cjson.decode, weights_json)
if ok and type(decoded) == 'table' then
  weights = decoded
end

local KEY_ACT = KEYS[5]
local KEY_AGG = KEYS[6]

local unpack = unpack or table.unpack

local function weight_of(t)
  local w = weights[t]
  if w == nil then w = default_w end
  if w == nil or w <= 0 then w = 1.0 end
  return w
end

local function ceil_ms_from_rate(deficit, rate)
  if deficit <= 0 then return 1 end
  if rate == nil or rate <= 0 then return 1000 end
  local ms = math.ceil((deficit / rate) * 1000.0)
  if ms < 1 then return 1 end
  return ms
end

-- Parse tb:tp hash value: "bucket_float:last_used_ms" or legacy plain bucket float.
local function parse_tp_stored(st)
  if st == nil or st == false then return 0.0, nil end
  st = tostring(st)
  local tb_str, ls_str = st:match("^(.+):(%d+)$")
  if tb_str and ls_str then
    return tonumber(tb_str) or 0.0, tonumber(ls_str)
  end
  return tonumber(st) or 0.0, nil
end

local function format_tp_stored(tb, ms)
  return tostring(tb) .. ":" .. tostring(ms)
end

local SWEEP_CAP = 50

local function registry_sweep()
  local last = redis.call('HGET', KEYS[4], '_sweep_ms')
  if last and (now_ms - tonumber(last) < idle_ms) then
    return
  end
  redis.call('HSET', KEYS[4], '_sweep_ms', now_ms)
  local entries = redis.call('HGETALL', KEYS[4])
  local evicted = 0
  local i = 1
  while i < #entries and evicted < SWEEP_CAP do
    local k = entries[i]
    local v = entries[i + 1]
    if k ~= '_sweep_ms' and k ~= '_tp_sweep_ms' and v then
      local ls = tonumber(v)
      if ls and (now_ms - ls > idle_ms) then
        redis.call('HDEL', KEYS[4], k)
        evicted = evicted + 1
      end
    end
    i = i + 2
  end
end

-- Evict stale per-(tenant,priority) bucket fields from the tp hash so they stop
-- inflating the fair-share denominator.  Rate-limited (once per idle_ms) and
-- capped, exactly as before Phase 2, the per-priority aggregate excludes stale
-- members from sum_w on every acquire, while this keeps the tp hash itself tidy
-- (so self refills from its stored value identically to the pre-aggregate code).
local function tp_sweep()
  if idle_ms == nil or idle_ms <= 0 then
    return
  end
  local last = redis.call('HGET', KEYS[4], '_tp_sweep_ms')
  if last and (now_ms - tonumber(last) < idle_ms) then
    return
  end
  redis.call('HSET', KEYS[4], '_tp_sweep_ms', now_ms)
  local entries = redis.call('HGETALL', KEYS[3])
  local evicted = 0
  local i = 1
  while i < #entries and evicted < SWEEP_CAP do
    local field = entries[i]
    local val = entries[i + 1]
    i = i + 2
    local _tb, last_used = parse_tp_stored(val)
    if last_used and (now_ms - last_used > idle_ms) then
      redis.call('HDEL', KEYS[3], field)
      evicted = evicted + 1
    end
  end
end

local function reconcile_tenant()
  redis.call('HSET', KEYS[4], tenant_id, now_ms)
  if idle_ms and idle_ms > 0 then
    redis.call('EXPIRE', KEYS[4], math.floor(idle_ms / 1000 * 10) + 60)
  end
end

local global_cap = limit_rps * burst_s * g_mult
local r1 = limit_rps * share1
local r2 = limit_rps * share2
local r3 = limit_rps * share3
local c1 = math.max(cost, r1 * burst_s)
local c2 = math.max(cost, r2 * burst_s)
local c3 = math.max(cost, r3 * burst_s)
local cap_tp = cost
local tp_field = tenant_id .. ':' .. pri_key

reconcile_tenant()
registry_sweep()
tp_sweep()

-- Self's stored tp bucket, read after tp_sweep so a swept (stale) self entry
-- refills cold, identical to the pre-aggregate read ordering.
local self_raw_tb = redis.call('HGET', KEYS[3], tp_field)

-- ---------------------------------------------------------------------------
-- Incremental per-priority active-weight aggregate (replaces the O(active) scan)
-- ---------------------------------------------------------------------------
-- Rebuild the aggregate for this priority from the tp hash (used on cold start,
-- config change, or periodic self-heal).  Mirrors the old full scan: stale rows
-- are excluded from the sum (and left for tp_sweep to delete), non-stale rows
-- seed the active set.
local stale_threshold = now_ms - idle_ms

local function rebuild_agg()
  redis.call('DEL', KEY_ACT)
  local entries = redis.call('HGETALL', KEYS[3])
  local sum = 0.0
  local i = 1
  while i < #entries do
    local field = entries[i]
    local val = entries[i + 1]
    i = i + 2
    local sep = string.find(field, ':')
    if sep then
      local t = string.sub(field, 1, sep - 1)
      local p = tonumber(string.sub(field, sep + 1))
      if p == pri then
        local _tb, last_used = parse_tp_stored(val)
        if not (last_used and (now_ms - last_used > idle_ms)) then
          local lu = last_used or now_ms
          redis.call('ZADD', KEY_ACT, lu, t)
          sum = sum + weight_of(t)
        end
      end
    end
  end
  return sum
end

local agg = redis.call('HMGET', KEY_AGG, 'sum_w', 'cfg_ver', 'rebuild_ms')
local sum_w
local rebuild_ms = tonumber(agg[3] or '0') or 0
local need_rebuild = (agg[1] == false) or (agg[2] ~= cfg_ver)
if not need_rebuild then
  if rebuild_ms == 0 or (now_ms - rebuild_ms) >= idle_ms then
    need_rebuild = true
  end
end
if need_rebuild then
  sum_w = rebuild_agg()
  rebuild_ms = now_ms
else
  sum_w = tonumber(agg[1]) or 0.0
  -- Evict newly-stale members from the aggregate (score < now - idle_ms):
  -- amortized O(1) per tenant.  The tp hash row is left for tp_sweep to delete,
  -- so self still refills from its stored value identically to the old scan.
  local stale = redis.call('ZRANGEBYSCORE', KEY_ACT, '-inf', '(' .. tostring(stale_threshold))
  for _, m in ipairs(stale) do
    sum_w = sum_w - weight_of(m)
    redis.call('ZREM', KEY_ACT, m)
  end
  if sum_w < 0 then sum_w = 0 end
end

local self_score = redis.call('ZSCORE', KEY_ACT, tenant_id)
local self_is_member = (self_score ~= false and self_score ~= nil)

-- scan-equivalent fair-share denominator: peers (incl. self if it has a live
-- bucket) plus self's weight when self has no live bucket yet.
local active_sum_w_p = sum_w
if not self_is_member then active_sum_w_p = active_sum_w_p + my_w end
if active_sum_w_p <= 0 then active_sum_w_p = my_w end

local rate_tp = limit_rps * share_p * (my_w / active_sum_w_p)

local agg_ttl = math.floor(idle_ms / 1000 * 10) + 60

-- Persist the aggregate before returning.  add_self=true (grant/release) refreshes
-- self's membership and folds my_w into sum_w on first appearance.
local function persist_agg(add_self)
  local s = sum_w
  if add_self then
    if not self_is_member then s = s + my_w end
    redis.call('ZADD', KEY_ACT, now_ms, tenant_id)
  end
  redis.call('HSET', KEY_AGG, 'sum_w', tostring(s), 'cfg_ver', cfg_ver, 'rebuild_ms', tostring(rebuild_ms))
  if idle_ms and idle_ms > 0 then
    redis.call('EXPIRE', KEY_AGG, agg_ttl)
    redis.call('EXPIRE', KEY_ACT, agg_ttl)
  end
end

-- Per-priority last-active timestamps, loaded once by refill_all (same HMGET as
-- the priority buckets) and read by peer_active without extra round-trips.
local pri_act1, pri_act2, pri_act3 = 0, 0, 0

local function refill_all()
  local g = redis.call('HMGET', KEYS[1], 'budget', 'last_ms')
  local gb = tonumber(g[1] or '0') or 0.0
  local last_ms = tonumber(g[2] or '0') or 0
  local elapsed = 0.0
  if last_ms > 0 then
    elapsed = math.max(0.0, (now_ms - last_ms) / 1000.0)
  end
  local cold = (last_ms == 0)

  if cold then
    gb = global_cap
  else
    gb = math.min(global_cap, gb + limit_rps * elapsed)
  end

  local pr = redis.call('HMGET', KEYS[2], '1', '2', '3', 'last_ms', '1_act', '2_act', '3_act')
  local p1 = tonumber(pr[1] or '0') or 0.0
  local p2 = tonumber(pr[2] or '0') or 0.0
  local p3 = tonumber(pr[3] or '0') or 0.0
  local pri_last = tonumber(pr[4] or '0') or 0
  pri_act1 = tonumber(pr[5] or '0') or 0
  pri_act2 = tonumber(pr[6] or '0') or 0
  pri_act3 = tonumber(pr[7] or '0') or 0

  if cold or pri_last == 0 then
    p1, p2, p3 = c1, c2, c3
  else
    local pri_elapsed = math.max(0.0, (now_ms - pri_last) / 1000.0)
    p1 = math.min(c1, p1 + r1 * pri_elapsed)
    p2 = math.min(c2, p2 + r2 * pri_elapsed)
    p3 = math.min(c3, p3 + r3 * pri_elapsed)
  end

  local tb, tp_last_ms = parse_tp_stored(self_raw_tb)
  if cold or tp_last_ms == nil then
    tb = cap_tp
  else
    local tp_elapsed = math.max(0.0, (now_ms - tp_last_ms) / 1000.0)
    tb = math.min(cap_tp, tb + rate_tp * tp_elapsed)
  end

  return gb, tb, p1, p2, p3
end

local function persist(gb, p1, p2, p3, tb)
  redis.call('HSET', KEYS[1], 'budget', tostring(gb), 'last_ms', tostring(now_ms))
  -- Bump per-priority active timestamp so cross-priority peers know we are still
  -- contending. Other priorities' "_act" fields are left untouched so they decay
  -- naturally as their respective tenants stop acquiring.
  redis.call('HSET', KEYS[2], '1', tostring(p1), '2', tostring(p2), '3', tostring(p3), 'last_ms', tostring(now_ms), pri_key .. '_act', tostring(now_ms))
  redis.call('HSET', KEYS[3], tp_field, format_tp_stored(tb, now_ms))
end

if op == 'acquire' then
  local gb, tb, p1, p2, p3 = refill_all()

  -- Hard gate 1: global capacity exhausted.
  -- Priority-weighted retry: higher priorities retry faster for preemptive access.
  if gb < cost then
    local retry_ms = ceil_ms_from_rate(cost - gb, limit_rps)
    -- Scale retry by inverse priority weight: P1 retries at 1x, P2 at ~1.7x, P3 at 2.5x.
    local priority_factor = 1.0 / share_p  -- P1: 1/0.5=2, P2: 1/0.3=3.3, P3: 1/0.2=5
    local min_factor = 1.0 / share1        -- Normalize so P1 gets factor=1
    retry_ms = math.ceil(retry_ms * priority_factor / min_factor)
    persist_agg(false)
    return {0, retry_ms}
  end

  -- Hard gate 2: per-tenant rate limit (work-conserving).
  -- A peer "has capacity" when its bucket is >= cost, meaning it could grant
  -- right now if it asked. If at least one peer is in that state, we should
  -- respect our own cap and defer to them so they can use their fair share.
  -- If no peer at this priority has reserve (all peers are saturated or absent),
  -- the priority-level token stream is otherwise unclaimed at this instant, so
  -- we absorb the slack instead of leaving the link idle. Cross-priority and
  -- global caps below (Gate 1 already, Gate 3 next) still apply.
  if tb < cost then
    -- Slow path only: scan the recent active peers for one with reserve capacity.
    local any_peer_has_capacity = false
    local recent = redis.call('ZRANGE', KEY_ACT, 0, -1)
    local fields = {}
    for _, m in ipairs(recent) do
      if m ~= tenant_id then fields[#fields + 1] = m .. ':' .. pri_key end
    end
    if #fields > 0 then
      local vals = redis.call('HMGET', KEYS[3], unpack(fields))
      for _, v in ipairs(vals) do
        local peer_tb = parse_tp_stored(v)
        if peer_tb >= cost then
          any_peer_has_capacity = true
          break
        end
      end
    end
    if any_peer_has_capacity then
      local retry_ms = ceil_ms_from_rate(cost - tb, rate_tp)
      persist_agg(false)
      return {0, retry_ms}
    end
    -- Borrow: clamp tb to cost so the debit lands at 0 (no negative credit drift).
    tb = cost
  end

  -- Soft gate 3: cross-priority fairness. If this priority's bucket is overdrawn
  -- AND another priority has capacity AND that priority has been active recently,
  -- yield to let them catch up. An idle priority (no acquire within pri_idle_ms)
  -- does NOT block us, its full bucket is treated as headroom we may absorb.
  local function peer_active(p_str)
    local act = pri_act3
    if p_str == '1' then act = pri_act1
    elseif p_str == '2' then act = pri_act2 end
    if act <= 0 then return false end
    return (now_ms - act) <= pri_idle_ms
  end

  local my_pri_bucket
  local my_pri_rate
  if pri == 1 then my_pri_bucket = p1; my_pri_rate = r1
  elseif pri == 2 then my_pri_bucket = p2; my_pri_rate = r2
  else my_pri_bucket = p3; my_pri_rate = r3 end

  if my_pri_bucket < cost then
    local other_has_capacity = false
    if pri ~= 1 and p1 >= cost and peer_active('1') then other_has_capacity = true end
    if pri ~= 2 and p2 >= cost and peer_active('2') then other_has_capacity = true end
    if pri ~= 3 and p3 >= cost and peer_active('3') then other_has_capacity = true end
    if other_has_capacity then
      local retry_ms = ceil_ms_from_rate(cost - my_pri_bucket, my_pri_rate)
      persist_agg(false)
      return {0, retry_ms}
    end
  end

  -- GRANT: debit global, per-tenant, and priority.
  gb = gb - cost
  tb = tb - cost
  -- Floor tb at 0: prevents accumulated credit from granting above rate.
  if tb < 0 then tb = 0 end
  if pri == 1 then p1 = p1 - cost
  elseif pri == 2 then p2 = p2 - cost
  else p3 = p3 - cost end
  -- Floor priority buckets at 0 so the Gate 3 retry hint stays bounded by 1/r_p.
  if p1 < 0 then p1 = 0 end
  if p2 < 0 then p2 = 0 end
  if p3 < 0 then p3 = 0 end

  persist(gb, p1, p2, p3, tb)
  persist_agg(true)
  return {1, now_ms}

elseif op == 'release' then
  local gb, tb, p1, p2, p3 = refill_all()

  gb = math.min(global_cap, gb + cost)
  if pri == 1 then
    p1 = math.min(c1, p1 + cost)
  elseif pri == 2 then
    p2 = math.min(c2, p2 + cost)
  else
    p3 = math.min(c3, p3 + cost)
  end
  tb = math.min(cap_tp, tb + cost)

  persist(gb, p1, p2, p3, tb)
  persist_agg(true)
  return {1}
end

return redis.error_reply('unknown op: ' .. tostring(op))
