with exploded as (
    -- Unnest the array into one row per element
    select id, val
    from price_tiers
    lateral view outer explode(tier_prices) li as val
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (order by val) as val_percent_rank,
        percent_rank() over (order by val nulls last) as val_percent_rank_2,
        cume_dist() over (order by val nulls last) as val_cume_dist
    from exploded
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by val_cume_dist desc nulls last) as val_cume_dist_rank,
        percent_rank() over (order by val_cume_dist desc nulls last) as val_cume_dist_percent_rank,
        rank() over (order by val_percent_rank desc nulls last) as val_percent_rank_rank
    from metrics
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        val,
        val_percent_rank,
        val_percent_rank_2,
        val_cume_dist,
        val_cume_dist_rank,
        val_cume_dist_percent_rank,
        val_percent_rank_rank,
        percent_rank() over (order by val) as val_percent_rank_3,
        dense_rank() over (order by val_cume_dist_percent_rank desc) as val_cume_dist_percent_rank_dense_rank
    from enriched
),
valid as (
    -- Basic cleansing and defaults
    select
        val,
        val_percent_rank,
        val_percent_rank_2,
        val_cume_dist,
        val_cume_dist_percent_rank,
        val_percent_rank_rank,
        val_percent_rank_3,
        round(val_cume_dist_percent_rank_dense_rank * 1.1, 2) as adjusted_val_cume_dist_percent_rank_dense_rank
    from scored
    where val_percent_rank_3 > 0.77
)
select
    val,
    val_percent_rank_2,
    val_cume_dist_percent_rank,
    val_percent_rank_rank
from valid;
