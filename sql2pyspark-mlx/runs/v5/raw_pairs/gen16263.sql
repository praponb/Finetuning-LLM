with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        k,
        value
    from carts
    lateral view outer explode(attributes) x as k, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        created_at,
        value,
        datediff(current_date(), created_at) as days_since_created_at,
        date_format(created_at, 'MMM yyyy') as created_at_label,
        coalesce(k, 'OTHER') as k_filled
    from flattened
    where k = 'utm_source'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        created_at,
        days_since_created_at,
        created_at_label,
        k_filled,
        round(days_since_created_at * 1.15, 2) as adjusted_days_since_created_at,
        date_trunc('month', created_at) as created_at_period
    from filtered
    where created_at > timestamp '2025-03-01 16:00:00'
      and days_since_created_at > 25
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(adjusted_days_since_created_at) over (order by cast(created_at as bigint) range between 900 preceding and current row) as adjusted_days_since_created_at_max_15m,
        max(days_since_created_at) over (order by cast(created_at_period as bigint) range between 7776000 preceding and current row) as days_since_created_at_max_90d,
        cume_dist() over (order by days_since_created_at) as days_since_created_at_cume_dist
    from valid
),
scored_carts as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (order by adjusted_days_since_created_at desc nulls last) as adjusted_days_since_created_at_percent_rank,
        dense_rank() over (order by days_since_created_at_cume_dist desc nulls first) as days_since_created_at_cume_dist_dense_rank
    from scored
),
scored_carts_2 as (
    -- Running and moving aggregates
    select
        created_at,
        days_since_created_at,
        created_at_label,
        days_since_created_at_max_90d,
        days_since_created_at_cume_dist,
        adjusted_days_since_created_at_percent_rank,
        days_since_created_at_cume_dist_dense_rank,
        sum(days_since_created_at_max_90d) over (order by cast(created_at_period as bigint) range between 7776000 preceding and current row) as days_since_created_at_max_90d_sum_90d,
        cume_dist() over (order by days_since_created_at_cume_dist_dense_rank desc nulls first) as days_since_created_at_cume_dist_dense_rank_cume_dist,
        max(days_since_created_at) over (order by cast(created_at_period as bigint) range between 604800 preceding and current row) as days_since_created_at_max_7d
    from scored_carts
    where created_at_period >= '2025-01-06'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(days_since_created_at_cume_dist) over (order by cast(created_at as bigint) range between 900 preceding and current row) as days_since_created_at_cume_dist_max_15m,
        max(adjusted_days_since_created_at_percent_rank) over (order by cast(created_at as bigint) range between 900 preceding and current row) as adjusted_days_since_created_at_percent_rank_max_15m,
        percent_rank() over (order by days_since_created_at_cume_dist_dense_rank desc nulls last) as days_since_created_at_cume_dist_dense_rank_percent_rank,
        avg(days_since_created_at_max_90d) over (order by cast(created_at as bigint) range between 7200 preceding and current row) as days_since_created_at_max_90d_avg_2h,
        percent_rank() over (order by days_since_created_at_cume_dist desc nulls last) as days_since_created_at_cume_dist_percent_rank
    from scored_carts_2
)
select
    created_at,
    days_since_created_at,
    created_at_label,
    days_since_created_at_max_90d,
    days_since_created_at_cume_dist,
    adjusted_days_since_created_at_percent_rank,
    days_since_created_at_cume_dist_dense_rank,
    days_since_created_at_max_90d_sum_90d,
    days_since_created_at_cume_dist_dense_rank_cume_dist
from metrics
order by created_at desc nulls last
