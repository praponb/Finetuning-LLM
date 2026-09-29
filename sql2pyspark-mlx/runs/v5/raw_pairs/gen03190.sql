with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view explode(tier_limits) x as val
)
-- Multi-level aggregation
select
    plan,
    count_if(plan = 'enterprise') as enterprise_events,
    count(*) filter (where currency = 'THB') as thb_rows,
    round(sum(val) / nullif(count(case when currency = 'THB' then 1 end), 0), 2) as val_per_thb,
    round(sum(val) / nullif(count(case when currency = 'USD' then 1 end), 0), 2) as val_per_usd
from exploded
group by plan;
