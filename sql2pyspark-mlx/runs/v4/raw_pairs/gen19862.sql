with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_prices) t as val
)
-- Running and moving aggregates
select
    *,
    rank() over (partition by plan order by val nulls last) as val_rank,
    avg(val) over (partition by plan) as plan_avg_val,
    min(val) over (partition by plan) as plan_min_val,
    rank() over (order by val desc nulls last) as val_rank_2
from flattened;
