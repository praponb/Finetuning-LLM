with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_limits) as val
    from price_tiers
)
-- Multi-level aggregation
select
    case when val >= 83 then 'LARGE' else 'SMALL' end as val_tier,
    avg(val) filter (where plan = 'enterprise') as enterprise_avg_val,
    max(val) filter (where currency = 'EUR') as eur_max_val
from flattened
group by 1
having avg(val) filter (where plan = 'enterprise') < 0
order by enterprise_avg_val;
