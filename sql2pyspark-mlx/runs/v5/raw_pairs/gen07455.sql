with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        val
    from price_tiers
    lateral view explode(tier_limits) x as val
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by val asc nulls last) as val_rank,
        rank() over w_part as val_rank_2,
        rank() over w_part_2 as val_rank_3,
        rank() over w_part as val_rank_4,
        percent_rank() over w_part_2 as val_percent_rank
    from flattened
    window w_part as (order by val desc), w_part_2 as (order by val desc nulls first)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        val_rank_2 * val_rank as val_rank_2_times_val_rank,
        round(val_rank_2, 2) as val_rank_2_rounded
    from enriched
    where val <> 69
),
scored as (
    -- Running and moving aggregates
    select
        id,
        val_rank,
        val_rank_2,
        val_rank_3,
        val_percent_rank,
        val_rank_2_times_val_rank,
        val_rank_2_rounded,
        rank() over (order by val_rank_2 asc nulls last) as val_rank_2_rank
    from cleaned
    where val_rank_2_times_val_rank between 37 and 57
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        val_rank_3,
        val_percent_rank,
        val_rank_2_times_val_rank,
        val_rank_2_rank,
        dense_rank() over (order by val_rank_2_times_val_rank desc nulls last) as val_rank_2_times_val_rank_dense_rank,
        cume_dist() over (order by val_rank_2_rounded asc) as val_rank_2_rounded_cume_dist,
        cume_dist() over (order by val_rank asc nulls last) as val_rank_cume_dist
    from scored
)
select
    id,
    val_rank_3,
    val_percent_rank,
    val_rank_2_times_val_rank,
    val_rank_2_rank,
    val_rank_2_times_val_rank_dense_rank,
    val_rank_cume_dist
from metrics
order by val_rank_3 desc
