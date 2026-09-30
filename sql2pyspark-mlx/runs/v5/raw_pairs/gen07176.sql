with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode(tier_limits) as val
    from price_tiers
),
filtered as (
    -- Basic cleansing and defaults
    select *, round(val * 0.9, 2) as val_scaled
    from lines
    where effective_from > date '2024-04-24'
      and effective_from >= '2024-01-20'
),
remaining_keys as (
    -- Set difference on keys
    select currency, id
    from filtered
    except distinct
    select currency, id
    from price_tiers
    where plan = 'basic'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join remaining_keys k on s.currency = k.currency and s.id = k.id
),
filtered_price_tiers as (
    -- Keep recent, valid rows
    select
        id,
        effective_from,
        currency,
        val,
        val_scaled,
        date_sub(effective_from, 46) as effective_from_minus_46d
    from kept_rows
    where effective_from >= date '2024-03-13'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        effective_from,
        val,
        val_scaled,
        effective_from_minus_46d,
        round(val, 2) as val_rounded
    from filtered_price_tiers
    where val_scaled >= 65
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by val nulls last) as val_rank,
        dense_rank() over w_grp as val_scaled_dense_rank,
        dense_rank() over w_grp as val_scaled_dense_rank_2
    from base
    window w_grp as (order by val_scaled desc nulls first)
),
base_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select *, least(val, val_rounded) as least_val_val_rounded
    from metrics
    where effective_from > date '2024-03-14'
),
grouped as (
    -- Multi-level aggregation
    select
        date_trunc('week', effective_from) as week_start,
        min(val_scaled_dense_rank) as min_val_scaled_dense_rank,
        max(val_scaled_dense_rank_2) as max_val_scaled_dense_rank_2,
        round(avg(val), 2) as avg_val,
        round(avg(val_scaled_dense_rank), 2) as avg_val_scaled_dense_rank,
        percentile_approx(least_val_val_rounded, 0.95) as least_val_val_rounded_p95,
        avg(val) as avg_val_2
    from base_price_tiers
    group by week_start
    having min_val_scaled_dense_rank < 1000000
)
select
    week_start,
    min_val_scaled_dense_rank,
    max_val_scaled_dense_rank_2,
    avg_val,
    avg_val_scaled_dense_rank,
    least_val_val_rounded_p95
from grouped;
