with lines as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode(tier_limits) as tier_value
    from price_tiers
)
-- Multi-level aggregation
select
    currency,
    count(*) filter (where currency = 'THB') as thb_rows,
    round(avg(tier_value), 2) as avg_tier_value
from lines
group by 1
having round(avg(tier_value), 2) > 0
order by currency desc;
