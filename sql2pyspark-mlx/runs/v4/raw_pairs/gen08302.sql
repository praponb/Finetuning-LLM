with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode(tier_limits) as val
    from price_tiers
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by currency order by val desc) as val_rank,
        cume_dist() over (partition by currency order by val nulls last) as val_cume_dist,
        percent_rank() over (partition by plan order by val nulls first) as val_percent_rank
    from flattened
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        currency,
        val_rank,
        val_cume_dist,
        round(val_cume_dist * 100, 2) as val_cume_dist_adjusted
    from scored
    where val_cume_dist >= 0.31
)
-- Native SQL PIVOT
select *
from (
    select
        plan,
        currency,
        val_cume_dist_adjusted
    from base
) src
pivot (
    sum(val_cume_dist_adjusted) as amount
    for currency in ('THB', 'EUR')
)
