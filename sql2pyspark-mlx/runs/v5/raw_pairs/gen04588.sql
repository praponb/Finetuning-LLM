with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view explode(discounts) t as val
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('YEAR', effective_from) as effective_from_period,
        case when plan = 'pro' then 1 else 0 end as is_pro
    from flattened
    where effective_from between '2025-03-01' and '2025-06-12'
),
metrics as (
    -- Window metrics over several different partitions
    select *, stddev_samp(is_pro) over (partition by plan) as plan_stddev_samp_is_pro
    from valid
),
remaining_keys as (
    -- Set intersection on keys
    select plan, id
    from metrics
    intersect all
    select plan, id
    from price_tiers
    where currency in ('USD', 'THB', 'EUR')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join remaining_keys k on s.plan = k.plan and s.id = k.id
)
-- Multi-level aggregation
select
    plan,
    date_trunc('year', effective_from_period) as year_start,
    count(distinct id) as distinct_ids,
    count(*) as cnt,
    max(effective_from_period) as last_seen
from verified
group by rollup (plan, date_trunc('year', effective_from_period))
having distinct_ids >= 1;
