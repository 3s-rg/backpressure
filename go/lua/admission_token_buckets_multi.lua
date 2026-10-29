-- Atomic multi-dimensional admission with DRF-aware per-tenant share.
--
-- Generalises admission_token_buckets.lua from one resource dimension (e.g.
-- RPM) to N independent dimensions (e.g. RPM + TPM).
--
-- Layout
-- ------
-- Per dim d ∈ {1..N}:
--   L1 (global, per dim): hard cap on aggregate (dim, all tenants).
--   L2 (per-priority, per dim): per-priority refill shares enforce the
--       configured cross-priority split for that dim with the same
--       work-conserving cross-priority yield as the single-dim gate.
-- Shared across dims:
--   L3 (per-(tenant, priority) DRF share-bucket): refills at the tenant's
--       weighted fair share of unit-share/sec, one *normalized-share* cost
--       per request equal to max_d (cost_d / C_d).  This enforces the
--       Dominant Resource Fairness invariant: each tenant's delivered
--       dominant share equals its weight share within priority.
--
-- The L3 fair-share denominator (sum of active weights) and the per-dim DRF
-- denominators are tracked incrementally in a per-priority sorted set (active
-- set) + aggregate hash + contribution hash, so the hot path is O(1) instead
-- of scanning the whole tp hash.  Stale members are evicted lazily, a config
-- change or periodic timer triggers a full rebuild, and the cross-priority
-- "any peer has capacity" check is computed only on the rate-limited slow path.
--
-- Key layout (KEYS array, 0-indexed):
--   KEYS[1] = bpq:{T}:tb:reg                      hash, tenant registry
--   KEYS[2] = bpq:{T}:tb:tp                       hash, shared DRF bucket
--   then, per dim d (d=1..N), two keys come in pairs:
--     KEYS[3 + 2*(d-1) + 0] = bpq:{T}:tb:global:{dim}    hash, L1
--     KEYS[3 + 2*(d-1) + 1] = bpq:{T}:tb:pri:{dim}       hash, L2
--   then three per-priority incremental-aggregate keys:
--     KEYS[2 + 2*N + 1] = bpq:{T}:tb:act:{pri}    zset  member=tenant, score=last_used_ms
--     KEYS[2 + 2*N + 2] = bpq:{T}:tb:agg:{pri}    hash  sum_w, cfg_ver, rebuild_ms, denom1..N
--     KEYS[2 + 2*N + 3] = bpq:{T}:tb:con:{pri}    hash  member=tenant -> stored cost vector
--
-- ARGV layout:
--   [1]  op              "acquire" | "release"
--   [2]  tenant_id
--   [3]  priority        1|2|3
--   [4]  now_ms
--   [5]  num_dims (N)
--   [6]  my_weight
--   [7]  tenant_weights_json
--   [8]  default_weight
--   [9]  idle_ms
--   [10] pri_idle_ms
--   [11] burst_seconds
--   [12] global_burst_mult
--   then for each dim d (d=1..N), six entries:
--     [12 + 6*(d-1) + 1] dim_name (informational, used in retry hint logs)
--     [12 + 6*(d-1) + 2] limit_rps_d
--     [12 + 6*(d-1) + 3] cost_d
--     [12 + 6*(d-1) + 4] share_1_d
--     [12 + 6*(d-1) + 5] share_2_d
--     [12 + 6*(d-1) + 6] share_3_d
--   then:
--     [12 + 6*N + 1] cfg_ver (aggregate fingerprint, mismatch forces rebuild)
--
-- Returns {granted, second, dim_name?} where:
--   granted=1, second=granted_at_ms on grant
--   granted=0, second=retry_after_ms, dim_name="<binding dim>|share|pri" on deny.

local op           = ARGV[1]
local tenant_id    = ARGV[2]
local pri_str      = ARGV[3]
local now_ms       = tonumber(ARGV[4])
local num_dims     = tonumber(ARGV[5])
local my_w         = tonumber(ARGV[6]) or 1.0
local weights_json = ARGV[7] or '{}'
local default_w    = tonumber(ARGV[8]) or 1.0
local idle_ms      = tonumber(ARGV[9]) or 10000
local pri_idle_ms  = tonumber(ARGV[10]) or 2000
local burst_s      = tonumber(ARGV[11]) or 1.0
local g_mult       = tonumber(ARGV[12]) or 1.2
local cfg_ver      = ARGV[12 + 6 * num_dims + 1] or ''

if my_w == nil or my_w <= 0 then my_w = 1.0 end

local pri = tonumber(pri_str) or 3
if pri < 1 then pri = 1 end
if pri > 3 then pri = 3 end
local pri_key = tostring(math.floor(pri))

-- Parse per-dim parameters.
local dims = {}
for d = 1, num_dims do
  local base = 12 + 6 * (d - 1)
  local dim = {
    name      = ARGV[base + 1],
    limit_rps = tonumber(ARGV[base + 2]) or 0.0,
    cost      = tonumber(ARGV[base + 3]) or 0.0,
    share1    = tonumber(ARGV[base + 4]) or 0.0,
    share2    = tonumber(ARGV[base + 5]) or 0.0,
    share3    = tonumber(ARGV[base + 6]) or 0.0,
    key_global = KEYS[3 + 2 * (d - 1) + 0],
    key_pri    = KEYS[3 + 2 * (d - 1) + 1],
  }
  if dim.share_p == nil then
    if pri == 1 then dim.share_p = dim.share1
    elseif pri == 2 then dim.share_p = dim.share2
    else dim.share_p = dim.share3 end
  end
  dim.r1 = dim.limit_rps * dim.share1
  dim.r2 = dim.limit_rps * dim.share2
  dim.r3 = dim.limit_rps * dim.share3
  dim.c1 = math.max(dim.cost, dim.r1 * burst_s)
  dim.c2 = math.max(dim.cost, dim.r2 * burst_s)
  dim.c3 = math.max(dim.cost, dim.r3 * burst_s)
  dim.r_p = dim.limit_rps * dim.share_p
  dim.global_cap = math.max(dim.cost, dim.limit_rps * burst_s * g_mult)
  table.insert(dims, dim)
end

-- DRF normalized cost: dominant dim is the one whose cost/limit is highest.
local share_cost = 0.0
local dominant_dim_name = '?'
for _, d in ipairs(dims) do
  if d.limit_rps > 0 then
    local s = d.cost / d.limit_rps
    if s > share_cost then
      share_cost = s
      dominant_dim_name = d.name
    end
  end
end
if share_cost <= 0 then
  -- All dims have zero cost, trivial grant.
  return {1, now_ms, ''}
end

-- This acquire's cost vector (in dim-order), reused for refill/persist and the
-- aggregate self-contribution.
local my_costs = {}
for d = 1, num_dims do my_costs[d] = dims[d].cost end

local weights = {}
local ok, decoded = pcall(cjson.decode, weights_json)
if ok and type(decoded) == 'table' then
  weights = decoded
end

local KEY_REG = KEYS[1]
local KEY_TP  = KEYS[2]
local KEY_ACT = KEYS[2 + 2 * num_dims + 1]
local KEY_AGG = KEYS[2 + 2 * num_dims + 2]
local KEY_CON = KEYS[2 + 2 * num_dims + 3]

local unpack = unpack or table.unpack

-- Aggregate hash field order: sum_w, cfg_ver, rebuild_ms, denom1..denomN.
local AGG_FIELDS = {'sum_w', 'cfg_ver', 'rebuild_ms'}
for d = 1, num_dims do AGG_FIELDS[#AGG_FIELDS + 1] = 'denom' .. d end

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

-- Parse stored value: "<bucket>:<last_ms>:<c_1>:<c_2>:...:<c_N>".  Returns
-- (bucket, last_ms, [costs]).  Legacy "<bucket>:<last_ms>" or "<bucket>"
-- forms parse with an empty cost vector.
local function parse_stored(st)
  if st == nil then return 0.0, nil, {} end
  st = tostring(st)
  local parts = {}
  for p in string.gmatch(st, "([^:]+)") do
    table.insert(parts, p)
  end
  if #parts < 1 then return 0.0, nil, {} end
  if #parts == 1 then return tonumber(parts[1]) or 0.0, nil, {} end
  local bucket = tonumber(parts[1]) or 0.0
  local last_ms = tonumber(parts[2])
  local costs = {}
  for i = 3, #parts do
    table.insert(costs, tonumber(parts[i]) or 0.0)
  end
  return bucket, last_ms, costs
end

local function format_stored(b, ms, costs)
  local out = tostring(b) .. ":" .. tostring(ms)
  for _, c in ipairs(costs) do
    out = out .. ":" .. tostring(c)
  end
  return out
end

-- Cost vector <-> contribution-hash value.  The con hash mirrors the cost
-- portion of each active member's tp row so a stale member's DRF denominator
-- contribution can be subtracted without re-reading (a possibly-swept) tp row.
local function parse_costs(st)
  local costs = {}
  if st == nil or st == false then return costs end
  for p in string.gmatch(tostring(st), "([^:]+)") do
    costs[#costs + 1] = tonumber(p) or 0.0
  end
  return costs
end

local function format_costs(costs)
  local parts = {}
  for d = 1, num_dims do parts[#parts + 1] = tostring(costs[d] or 0.0) end
  return table.concat(parts, ':')
end

-- Compute share_cost from a cost vector and the dim limits.
local function compute_share_cost(costs, dims_arr)
  local s = 0.0
  for i = 1, math.min(#costs, #dims_arr) do
    local d = dims_arr[i]
    if d.limit_rps > 0 then
      local v = costs[i] / d.limit_rps
      if v > s then s = v end
    end
  end
  return s
end

local SWEEP_CAP = 50

local function registry_sweep()
  local last = redis.call('HGET', KEY_REG, '_sweep_ms')
  if last and (now_ms - tonumber(last) < idle_ms) then
    return
  end
  redis.call('HSET', KEY_REG, '_sweep_ms', now_ms)
  local entries = redis.call('HGETALL', KEY_REG)
  local evicted = 0
  local i = 1
  while i < #entries and evicted < SWEEP_CAP do
    local k = entries[i]; local v = entries[i + 1]
    if k ~= '_sweep_ms' and k ~= '_tp_sweep_ms' and v then
      local ls = tonumber(v)
      if ls and (now_ms - ls > idle_ms) then
        redis.call('HDEL', KEY_REG, k)
        evicted = evicted + 1
      end
    end
    i = i + 2
  end
end

local function tp_sweep()
  if idle_ms == nil or idle_ms <= 0 then return end
  local last = redis.call('HGET', KEY_REG, '_tp_sweep_ms')
  if last and (now_ms - tonumber(last) < idle_ms) then return end
  redis.call('HSET', KEY_REG, '_tp_sweep_ms', now_ms)
  local entries = redis.call('HGETALL', KEY_TP)
  local evicted = 0
  local i = 1
  while i < #entries and evicted < SWEEP_CAP do
    local field = entries[i]; local val = entries[i + 1]
    i = i + 2
    local _b, last_used, _c = parse_stored(val)
    if last_used and (now_ms - last_used > idle_ms) then
      redis.call('HDEL', KEY_TP, field)
      evicted = evicted + 1
    end
  end
end

local function reconcile_tenant()
  redis.call('HSET', KEY_REG, tenant_id, now_ms)
  if idle_ms and idle_ms > 0 then
    redis.call('EXPIRE', KEY_REG, math.floor(idle_ms / 1000 * 10) + 60)
  end
end

local tp_field = tenant_id .. ':' .. pri_key

-- ---------------------------------------------------------------------------
-- Incremental per-priority DRF aggregate (replaces the O(active) tp scan)
-- ---------------------------------------------------------------------------
-- Upvalues populated by prepare(), consumed by persist_agg().
local agg_sum_w = 0.0          -- post-eviction sum of active weights (excl self-current adj)
local agg_denom = {}           -- post-eviction per-dim DRF denominators
local agg_rebuild_ms = 0
local agg_self_member = false  -- was self already an active member this call?
local agg_self_costs = {}      -- self's stored cost vector (for the self-replacement)

-- Full rebuild from the tp hash.  Mirrors the old scan: stale rows are excluded
-- and non-stale rows seed the active set, contribution hash, and aggregates with
-- their STORED cost vectors (the per-call self-replacement swaps self's stored
-- contribution for the current request's below).
local function rebuild_agg()
  redis.call('DEL', KEY_ACT)
  redis.call('DEL', KEY_CON)
  local entries = redis.call('HGETALL', KEY_TP)
  local sw = 0.0
  local dn = {}
  for d = 1, num_dims do dn[d] = 0.0 end
  local i = 1
  while i < #entries do
    local field = entries[i]; local val = entries[i + 1]
    i = i + 2
    local sep = string.find(field, ':')
    if sep then
      local t = string.sub(field, 1, sep - 1)
      local p = tonumber(string.sub(field, sep + 1))
      if p == pri then
        local _b, last_used, costs = parse_stored(val)
        if not (last_used and (now_ms - last_used > idle_ms)) then
          local lu = last_used or now_ms
          local w = weight_of(t)
          redis.call('ZADD', KEY_ACT, lu, t)
          redis.call('HSET', KEY_CON, t, format_costs(costs))
          sw = sw + w
          local sc = compute_share_cost(costs, dims)
          if sc > 0 then
            for d = 1, num_dims do
              local c = costs[d]
              if c and c > 0 then dn[d] = dn[d] + w * c / sc end
            end
          end
        end
      end
    end
  end
  return sw, dn
end

-- prepare(): load/refresh the aggregate and return the DRF knob x_star, the
-- fair-share denominator eff_total (active weight incl. self at the CURRENT
-- request's cost vector), and self's raw stored tp value (for refill_tp).
local function prepare()
  local self_raw = redis.call('HGET', KEY_TP, tp_field)
  local _sb, _slu, self_stored_costs = parse_stored(self_raw)

  local agg = redis.call('HMGET', KEY_AGG, unpack(AGG_FIELDS))
  local sum_w
  local denom = {}
  local rebuild_ms = tonumber(agg[3] or '0') or 0
  local need_rebuild = (agg[1] == false) or (agg[2] ~= cfg_ver)
  if not need_rebuild then
    if rebuild_ms == 0 or (now_ms - rebuild_ms) >= idle_ms then
      need_rebuild = true
    end
  end
  if need_rebuild then
    sum_w, denom = rebuild_agg()
    rebuild_ms = now_ms
  else
    sum_w = tonumber(agg[1]) or 0.0
    for d = 1, num_dims do denom[d] = tonumber(agg[3 + d]) or 0.0 end
    -- Evict newly-stale members (score < now - idle_ms): amortized O(1) per
    -- tenant.  Contributions come from the con hash, never the (possibly swept)
    -- tp row, so the subtraction always reverses exactly what was added.
    local stale = redis.call('ZRANGEBYSCORE', KEY_ACT, '-inf', '(' .. tostring(now_ms - idle_ms))
    if #stale > 0 then
      local cons = redis.call('HMGET', KEY_CON, unpack(stale))
      for idx = 1, #stale do
        local m = stale[idx]
        local costs = parse_costs(cons[idx])
        local w = weight_of(m)
        sum_w = sum_w - w
        local sc = compute_share_cost(costs, dims)
        if sc > 0 then
          for d = 1, num_dims do
            local c = costs[d]
            if c and c > 0 then denom[d] = denom[d] - w * c / sc end
          end
        end
        redis.call('ZREM', KEY_ACT, m)
        redis.call('HDEL', KEY_CON, m)
      end
      if sum_w < 0 then sum_w = 0 end
      for d = 1, num_dims do if denom[d] < 0 then denom[d] = 0 end end
    end
  end

  local self_score = redis.call('ZSCORE', KEY_ACT, tenant_id)
  local self_member = (self_score ~= false and self_score ~= nil)

  -- Stash post-eviction aggregate for persist_agg().
  agg_sum_w = sum_w
  agg_denom = denom
  agg_rebuild_ms = rebuild_ms
  agg_self_member = self_member
  agg_self_costs = self_stored_costs

  -- Effective denominator: count self at the CURRENT request's cost vector,
  -- replacing its stored contribution (baseline scan includes self via my_costs).
  local eff_total = sum_w
  if not self_member then eff_total = eff_total + my_w end

  local eff_denom = {}
  for d = 1, num_dims do eff_denom[d] = denom[d] end
  if self_member then
    local sc_old = compute_share_cost(self_stored_costs, dims)
    if sc_old > 0 then
      for d = 1, num_dims do
        local c = self_stored_costs[d]
        if c and c > 0 then eff_denom[d] = eff_denom[d] - my_w * c / sc_old end
      end
    end
  end
  if share_cost > 0 then
    for d = 1, num_dims do
      local c = dims[d].cost
      if c and c > 0 then eff_denom[d] = eff_denom[d] + my_w * c / share_cost end
    end
  end
  for d = 1, num_dims do if eff_denom[d] < 0 then eff_denom[d] = 0 end end

  if eff_total <= 0 then eff_total = my_w end

  -- DRF equalisation knob X*: smallest per-dim capacity ratio.
  local x_star = 1.0e18
  for d = 1, num_dims do
    local dim_d = dims[d]
    if eff_denom[d] > 0 then
      local ratio = (dim_d.limit_rps * eff_total) / eff_denom[d]
      if ratio < x_star then x_star = ratio end
    end
  end
  if x_star <= 0 or x_star >= 1.0e18 then
    x_star = 1.0
  end

  return x_star, eff_total, self_raw
end

-- Persist the aggregate.  add_self=true (grant/release) refreshes self's
-- membership and folds the current request's contribution in (replacing any
-- stored one), add_self=false (deny) just persists the post-eviction state.
local function persist_agg(add_self)
  local s = agg_sum_w
  local dn = {}
  for d = 1, num_dims do dn[d] = agg_denom[d] end
  if add_self then
    if not agg_self_member then s = s + my_w end
    if agg_self_member then
      local sc_old = compute_share_cost(agg_self_costs, dims)
      if sc_old > 0 then
        for d = 1, num_dims do
          local c = agg_self_costs[d]
          if c and c > 0 then dn[d] = dn[d] - my_w * c / sc_old end
        end
      end
    end
    if share_cost > 0 then
      for d = 1, num_dims do
        local c = dims[d].cost
        if c and c > 0 then dn[d] = dn[d] + my_w * c / share_cost end
      end
    end
    for d = 1, num_dims do if dn[d] < 0 then dn[d] = 0 end end
    redis.call('ZADD', KEY_ACT, now_ms, tenant_id)
    redis.call('HSET', KEY_CON, tenant_id, format_costs(my_costs))
  end
  local args = {'sum_w', tostring(s), 'cfg_ver', cfg_ver, 'rebuild_ms', tostring(agg_rebuild_ms)}
  for d = 1, num_dims do
    args[#args + 1] = 'denom' .. d
    args[#args + 1] = tostring(dn[d])
  end
  redis.call('HSET', KEY_AGG, unpack(args))
  if idle_ms and idle_ms > 0 then
    local ttl = math.floor(idle_ms / 1000 * 10) + 60
    redis.call('EXPIRE', KEY_AGG, ttl)
    redis.call('EXPIRE', KEY_ACT, ttl)
    redis.call('EXPIRE', KEY_CON, ttl)
  end
end

-- Slow-path-only "does any actively-contending peer at this priority have L3
-- capacity >= share_cost?".  Scans only the recency tail (last_used within
-- pri_idle_ms) of the active set, never the whole tp hash, and is consulted
-- only when the requester's own share-bucket is depleted.
local function slow_path_any_peer()
  local recent = redis.call('ZRANGEBYSCORE', KEY_ACT, tostring(now_ms - pri_idle_ms), '+inf')
  local fields = {}
  for _, m in ipairs(recent) do
    if m ~= tenant_id then fields[#fields + 1] = m .. ':' .. pri_key end
  end
  if #fields == 0 then return false end
  local vals = redis.call('HMGET', KEY_TP, unpack(fields))
  for _, v in ipairs(vals) do
    local peer_bucket = parse_stored(v)
    if peer_bucket >= share_cost then return true end
  end
  return false
end

-- Per-dim L1 refill.
local function refill_global(dim)
  local g = redis.call('HMGET', dim.key_global, 'budget', 'last_ms')
  local gb = tonumber(g[1] or '0') or 0.0
  local last_ms = tonumber(g[2] or '0') or 0
  local cold = (last_ms == 0)
  if cold then
    gb = dim.global_cap
  else
    local elapsed = math.max(0.0, (now_ms - last_ms) / 1000.0)
    gb = math.min(dim.global_cap, gb + dim.limit_rps * elapsed)
  end
  return gb
end

-- Per-dim L2 refill.  Returns p1, p2, p3.  Also stashes this dim's per-priority
-- last-active timestamps on the dim table (loaded in the same HMGET) so
-- peer_pri_active needs no extra round-trip.
local function refill_pri(dim)
  local pr = redis.call('HMGET', dim.key_pri, '1', '2', '3', 'last_ms', '1_act', '2_act', '3_act')
  local p1 = tonumber(pr[1] or '0') or 0.0
  local p2 = tonumber(pr[2] or '0') or 0.0
  local p3 = tonumber(pr[3] or '0') or 0.0
  local pri_last = tonumber(pr[4] or '0') or 0
  dim.act1 = tonumber(pr[5] or '0') or 0
  dim.act2 = tonumber(pr[6] or '0') or 0
  dim.act3 = tonumber(pr[7] or '0') or 0
  local cold = (pri_last == 0)
  if cold then
    p1, p2, p3 = dim.c1, dim.c2, dim.c3
  else
    local elapsed = math.max(0.0, (now_ms - pri_last) / 1000.0)
    p1 = math.min(dim.c1, p1 + dim.r1 * elapsed)
    p2 = math.min(dim.c2, p2 + dim.r2 * elapsed)
    p3 = math.min(dim.c3, p3 + dim.r3 * elapsed)
  end
  return p1, p2, p3
end

local function peer_pri_active(dim, p_str)
  local act = dim.act3
  if p_str == '1' then act = dim.act1
  elseif p_str == '2' then act = dim.act2 end
  if act == nil or act <= 0 then return false end
  return (now_ms - act) <= pri_idle_ms
end

-- Shared per-tenant DRF share-bucket refill.
local function refill_tp(rate_share, raw)
  -- Bucket capacity is the larger of share_cost (must fit one current request)
  -- or one burst-second of refill rate.  *raw* is self's stored value captured
  -- by prepare()'s HGET, avoiding a separate read.
  local cap = math.max(share_cost, rate_share * burst_s)
  local b, last_used, _stored_costs = parse_stored(raw)
  if last_used == nil then
    b = cap
  else
    local elapsed = math.max(0.0, (now_ms - last_used) / 1000.0)
    b = math.min(cap, b + rate_share * elapsed)
  end
  return b, cap
end

if op == 'acquire' then
  reconcile_tenant()
  registry_sweep()
  tp_sweep()

  local x_star, eff_total, self_raw_tp = prepare()
  -- DRF max-min refill: (w_t / W_p) * X*  share-units / sec.  X* equalises
  -- dominant shares across active tenants at this priority while saturating
  -- the binding L1 dim.  When tenants are homogeneous (same cost vector),
  -- X* reduces to share_cost / (1 / W_p) and rate_share collapses to the
  -- WFQ-per-dim ideal of the simpler 1/N formulation.
  local rate_share = (my_w / eff_total) * x_star
  local tp_bucket, _tp_cap = refill_tp(rate_share, self_raw_tp)

  -- Per-dim L1, L2 refills + checks.
  local max_retry = 0
  local binding = ''
  local dim_state = {}

  for di, d in ipairs(dims) do
    local gb = refill_global(d)
    local p1, p2, p3 = refill_pri(d)
    local my_pri_bucket
    local my_pri_rate
    if pri == 1 then my_pri_bucket = p1; my_pri_rate = d.r1
    elseif pri == 2 then my_pri_bucket = p2; my_pri_rate = d.r2
    else my_pri_bucket = p3; my_pri_rate = d.r3 end
    dim_state[di] = {gb = gb, p1 = p1, p2 = p2, p3 = p3,
                     my_pri_bucket = my_pri_bucket, my_pri_rate = my_pri_rate}

    -- Gate 1: global cap.
    if gb < d.cost then
      local r = ceil_ms_from_rate(d.cost - gb, d.limit_rps)
      -- Priority-weighted retry (carry over from single-dim Lua).
      if d.share_p > 0 and d.share1 > 0 then
        local pf = 1.0 / d.share_p
        local mf = 1.0 / d.share1
        r = math.ceil(r * pf / mf)
      end
      if r > max_retry then max_retry = r; binding = d.name .. ':global' end
    end

    -- Gate 2: per-priority soft yield.
    if my_pri_bucket < d.cost then
      local other_has_capacity = false
      if pri ~= 1 and p1 >= d.cost and peer_pri_active(d, '1') then other_has_capacity = true end
      if pri ~= 2 and p2 >= d.cost and peer_pri_active(d, '2') then other_has_capacity = true end
      if pri ~= 3 and p3 >= d.cost and peer_pri_active(d, '3') then other_has_capacity = true end
      if other_has_capacity then
        local r = ceil_ms_from_rate(d.cost - my_pri_bucket, my_pri_rate)
        if r > max_retry then max_retry = r; binding = d.name .. ':pri' end
      end
    end
  end

  -- Gate 3: DRF L3 (shared share-bucket).
  -- Work-conserving borrow: when NO active peer at this priority has L3
  -- capacity >= share_cost AND is actively contending (last_used within
  -- pri_idle_ms), the requesting tenant may proceed even if its own
  -- share-bucket is depleted.
  --
  -- Special case: single-dimension mode (num_dims == 1). L1 (global cap) and
  -- L2 (per-priority) already provide the correct throughput ceiling and cross-
  -- priority isolation for the single resource dimension. L3's DRF is only
  -- meaningful when N>=2 to balance dominant shares across heterogeneous cost
  -- vectors. In single-dim, all tenants have identical cost vectors (cost/cap
  -- is the same), so DRF collapses to simple WFQ. However, strict L3 in single-
  -- dim blocks work-conserving yield (e.g. tenant_busy can't absorb
  -- tenant_quiet's unused slack). Therefore, for N==1, L3 is non-binding:
  -- we skip the share-bucket check entirely, relying on L1+L2 for correctness.
  --
  -- For N>=2 (true multi-dim DRF): when at least one peer IS actively
  -- contending and has capacity, L3 is strict, enforcing DRF max-min fairness.
  -- The any-peer-has-capacity check is computed here, on the rate-limited slow
  -- path only, so the granted path never scans peers.
  if num_dims >= 2 and tp_bucket < share_cost then
    if slow_path_any_peer() then
      local r = ceil_ms_from_rate(share_cost - tp_bucket, rate_share)
      if r > max_retry then max_retry = r; binding = 'share' end
    end
  end

  if max_retry > 0 then
    persist_agg(false)
    return {0, max_retry, binding}
  end

  -- GRANT: debit all per-dim L1/L2 and the shared L3 share-bucket.
  for di, d in ipairs(dims) do
    local st = dim_state[di]
    local gb = st.gb - d.cost
    local p1, p2, p3 = st.p1, st.p2, st.p3
    if pri == 1 then p1 = p1 - d.cost
    elseif pri == 2 then p2 = p2 - d.cost
    else p3 = p3 - d.cost end
    if p1 < 0 then p1 = 0 end
    if p2 < 0 then p2 = 0 end
    if p3 < 0 then p3 = 0 end

    redis.call('HSET', d.key_global, 'budget', tostring(gb), 'last_ms', tostring(now_ms))
    redis.call('HSET', d.key_pri,
               '1', tostring(p1), '2', tostring(p2), '3', tostring(p3),
               'last_ms', tostring(now_ms),
               pri_key .. '_act', tostring(now_ms))
  end

  tp_bucket = tp_bucket - share_cost
  if tp_bucket < 0 then tp_bucket = 0 end
  redis.call('HSET', KEY_TP, tp_field, format_stored(tp_bucket, now_ms, my_costs))
  persist_agg(true)

  return {1, now_ms, dominant_dim_name}

elseif op == 'release' then
  reconcile_tenant()
  local x_star, eff_total, self_raw_tp = prepare()
  local rate_share = (my_w / eff_total) * x_star
  local tp_bucket, tp_cap = refill_tp(rate_share, self_raw_tp)

  for _, d in ipairs(dims) do
    local gb = refill_global(d)
    local p1, p2, p3 = refill_pri(d)
    gb = math.min(d.global_cap, gb + d.cost)
    if pri == 1 then p1 = math.min(d.c1, p1 + d.cost)
    elseif pri == 2 then p2 = math.min(d.c2, p2 + d.cost)
    else p3 = math.min(d.c3, p3 + d.cost) end
    redis.call('HSET', d.key_global, 'budget', tostring(gb), 'last_ms', tostring(now_ms))
    redis.call('HSET', d.key_pri,
               '1', tostring(p1), '2', tostring(p2), '3', tostring(p3),
               'last_ms', tostring(now_ms),
               pri_key .. '_act', tostring(now_ms))
  end

  tp_bucket = math.min(tp_cap, tp_bucket + share_cost)
  redis.call('HSET', KEY_TP, tp_field, format_stored(tp_bucket, now_ms, my_costs))
  persist_agg(true)

  return {1, now_ms, ''}
end

return redis.error_reply('unknown op: ' .. tostring(op))
