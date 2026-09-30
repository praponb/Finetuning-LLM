with lines as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        val
    from price_tiers
    lateral view explode(tier_prices) x as val
),
scored as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by val) as val_rank,
        dense_rank() over (order by val nulls first) as val_dense_rank,
        percent_rank() over (partition by currency order by val desc nulls first) as val_percent_rank,
        rank() over (order by val desc) as val_rank_2
    from lines
),
with_windows as (
    -- Running and moving aggregates
    select *, round(val_dense_rank / sum(val_dense_rank) over (partition by currency), 4) as val_dense_rank_share
    from scored
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        currency,
        val_rank,
        val_rank_2,
        val_dense_rank_share,
        case when val_rank >= 7 then 'High' when val_rank >= 4 then 'Medium' else 'Low' end as val_rank_band
    from with_windows
    where val_dense_rank < 6
)
select val_dense_rank_share, val_rank_band
from filtered
