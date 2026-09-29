with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode(tier_limits) as val
    from price_tiers
)
-- Multi-level aggregation
select
    case when val >= 68 then 'PREMIUM' else 'STANDARD' end as val_tier,
    currency,
    count(*) filter (where currency = 'THB') as thb_rows,
    count(*) filter (where currency = 'USD') as usd_rows,
    round(avg(val), 2) as avg_val
from exploded
group by val_tier, currency
