with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(discounts) as val
    from price_tiers
),
valid as (
    -- Basic cleansing and defaults
    select *, datediff(current_date(), effective_from) as days_since_effective_from
    from flattened
    where effective_from >= date '2024-04-01'
      and plan = 'enterprise'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(val) over (partition by plan) as plan_stddev_samp_val,
        dense_rank() over (order by val desc nulls first) as val_dense_rank,
        rank() over (partition by currency order by days_since_effective_from) as days_since_effective_from_rank
    from valid
),
scored as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over w_part as plan_stddev_samp_val_cume_dist,
        percent_rank() over (partition by currency, plan order by days_since_effective_from_rank desc) as days_since_effective_from_rank_percent_rank,
        percent_rank() over (partition by currency, plan order by days_since_effective_from desc nulls first) as days_since_effective_from_percent_rank,
        percent_rank() over (partition by currency order by val_dense_rank desc nulls first) as val_dense_rank_percent_rank
    from enriched
    window w_part as (partition by plan order by plan_stddev_samp_val desc nulls first)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, days_since_effective_from_rank_percent_rank * days_since_effective_from_percent_rank as days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank
    from scored
    where val < 67
),
remaining_keys as (
    -- Set intersection on keys
    select id, plan
    from filtered
    intersect
    select id, plan
    from price_tiers
    where currency = 'EUR'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.id = k.id and s.plan = k.plan
),
scored_price_tiers as (
    -- Running and moving aggregates
    select
        id,
        days_since_effective_from,
        val_dense_rank,
        val_dense_rank_percent_rank,
        days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank,
        rank() over (partition by currency order by plan_stddev_samp_val_cume_dist) as plan_stddev_samp_val_cume_dist_rank
    from reconciled
)
-- Multi-level aggregation
select
    case when val_dense_rank_percent_rank >= 0.63 then 'LARGE' else 'SMALL' end as val_dense_rank_percent_rank_tier,
    case when days_since_effective_from >= 78 then 'HIGH' else 'LOW' end as days_since_effective_from_tier,
    approx_percentile(val_dense_rank, 0.95) as val_dense_rank_p95,
    avg(days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank) as avg_days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank,
    approx_percentile(val_dense_rank, 0.9) as val_dense_rank_p90,
    grouping(case when val_dense_rank_percent_rank >= 0.63 then 'LARGE' else 'SMALL' end) as grouping_level
from scored_price_tiers
group by cube (case when val_dense_rank_percent_rank >= 0.63 then 'LARGE' else 'SMALL' end, case when days_since_effective_from >= 78 then 'HIGH' else 'LOW' end);
