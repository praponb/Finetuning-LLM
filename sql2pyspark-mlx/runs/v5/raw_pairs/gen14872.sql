with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_prices) li as val
),
metrics as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by val desc nulls first) val_rank,
        rank() over (partition by plan order by val) val_rank_2,
        max(val) over (partition by plan, currency) plan_max_val
    from lines
),
scored as (
    -- Ranking and gap features
    select
        effective_from,
        currency,
        val_rank_2,
        max(plan_max_val) over (partition by plan) plan_max_plan_max_val,
        cume_dist() over (partition by currency order by val_rank desc) val_rank_cume_dist,
        percent_rank() over (order by val_rank_2 desc) val_rank_2_percent_rank,
        max(val) over (partition by currency) currency_max_val,
        rank() over (partition by plan order by val) val_rank_3
    from metrics
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        effective_from,
        val_rank_2,
        plan_max_plan_max_val,
        val_rank_cume_dist,
        currency_max_val,
        val_rank_3,
        round(val_rank_2_percent_rank * 1.15, 2) val_rank_2_percent_rank_scaled
    from scored
    where effective_from < '2024-05-01'
),
enriched as (
    -- Running and moving aggregates
    select
        effective_from,
        val_rank_3,
        val_rank_2_percent_rank_scaled,
        rank() over (order by plan_max_plan_max_val desc nulls first) plan_max_plan_max_val_rank
    from base
    where currency_max_val <= 59
),
metrics_price_tiers as (
    -- Running and moving aggregates
    select *, dense_rank() over (order by val_rank_2_percent_rank_scaled nulls first) val_rank_2_percent_rank_scaled_dense_rank
    from enriched
)
-- Multi-level aggregation
select
    date_trunc('quarter', effective_from) as quarter_start,
    case when val_rank_2_percent_rank_scaled >= 83 then 'HIGH' else 'LOW' end as val_rank_2_percent_rank_scaled_tier,
    date_trunc('week', effective_from) as week_start,
    sum(val_rank_2_percent_rank_scaled_dense_rank) as total_val_rank_2_percent_rank_scaled_dense_rank,
    percentile_approx(val_rank_2_percent_rank_scaled_dense_rank, 0.95) as val_rank_2_percent_rank_scaled_dense_rank_p95,
    approx_percentile(val_rank_2_percent_rank_scaled, 0.9) as val_rank_2_percent_rank_scaled_p90
from metrics_price_tiers
group by cube (date_trunc('quarter', effective_from), case when val_rank_2_percent_rank_scaled >= 83 then 'HIGH' else 'LOW' end, date_trunc('week', effective_from))
order by val_rank_2_percent_rank_scaled_tier desc nulls first;
