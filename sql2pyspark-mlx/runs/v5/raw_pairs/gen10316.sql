with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(discounts) as val
    from price_tiers
)
-- Multi-level aggregation
select
    currency,
    plan,
    count(case when plan = 'basic' then 1 end) as basic_count,
    count(distinct currency) as distinct_currencies,
    round(avg(val), 2) as avg_val,
    sum(val) filter (where currency = 'USD') as usd_total_val
from flattened
group by currency, plan;
