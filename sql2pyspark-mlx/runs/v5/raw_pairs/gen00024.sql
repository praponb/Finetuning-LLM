with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        val
    from price_tiers
    lateral view explode(tier_limits) t as val
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        val,
        cume_dist() over w_recent as val_cume_dist
    from flattened
    window w_recent as (partition by plan order by val desc nulls first)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        val,
        dense_rank() over (order by val nulls first) as val_dense_rank,
        dense_rank() over (order by val_cume_dist desc nulls last) as val_cume_dist_dense_rank,
        percent_rank() over (order by val_cume_dist desc nulls first) as val_cume_dist_percent_rank
    from metrics
    where val = 40
),
scored_price_tiers as (
    -- Ranking and gap features
    select
        id,
        val,
        val_dense_rank,
        val_cume_dist_percent_rank,
        cume_dist() over (order by val_cume_dist_percent_rank desc nulls last) as val_cume_dist_percent_rank_cume_dist,
        cume_dist() over (order by val_cume_dist_dense_rank desc nulls first) as val_cume_dist_dense_rank_cume_dist,
        percent_rank() over (order by val_cume_dist_percent_rank desc) as val_cume_dist_percent_rank_percent_rank
    from scored
),
filtered as (
    -- Basic cleansing and defaults
    select *, val_cume_dist_percent_rank + val_cume_dist_percent_rank_cume_dist as val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist
    from scored_price_tiers
    where val_cume_dist_percent_rank > 0.64
),
filtered_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select *, round(val, 0) as val_rounded
    from filtered
    where val_dense_rank is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        val,
        val_cume_dist_percent_rank,
        val_cume_dist_dense_rank_cume_dist,
        val_cume_dist_percent_rank_percent_rank,
        val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist,
        dense_rank() over (order by val_rounded nulls last) as val_rounded_dense_rank,
        dense_rank() over w_hist as val_cume_dist_percent_rank_dense_rank
    from filtered_price_tiers
    window w_hist as (order by val_cume_dist_percent_rank)
)
select
    val,
    val_cume_dist_dense_rank_cume_dist,
    val_cume_dist_percent_rank_percent_rank,
    val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist,
    val_rounded_dense_rank,
    val_cume_dist_percent_rank_dense_rank
from with_windows
order by val nulls first;
