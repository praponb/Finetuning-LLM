with flattened as (
    -- Unnest the array into one row per element
    select id, tier_value
    from price_tiers
    lateral view explode(tier_limits) x as tier_value
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over w_recent as tier_value_dense_rank,
        rank() over w_running as tier_value_rank,
        percent_rank() over (order by tier_value desc nulls last) as tier_value_percent_rank,
        rank() over (order by tier_value nulls last) as tier_value_rank_2,
        dense_rank() over w_recent as tier_value_dense_rank_2
    from flattened
    window w_recent as (order by tier_value desc nulls first), w_running as (order by tier_value)
),
enriched_price_tiers as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by tier_value_dense_rank_2) as tier_value_dense_rank_2_rank,
        rank() over (order by tier_value_percent_rank desc nulls last) as tier_value_percent_rank_rank,
        percent_rank() over (order by tier_value_dense_rank nulls first) as tier_value_dense_rank_percent_rank
    from enriched
),
scored as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by tier_value) as tier_value_rank_3,
        percent_rank() over (order by tier_value_dense_rank_percent_rank nulls first) as tier_value_dense_rank_percent_rank_percent_rank
    from enriched_price_tiers
)
-- Multi-level aggregation
select
    case when tier_value_dense_rank_percent_rank_percent_rank >= 0.69 then 'HIGH' else 'LOW' end as tier_value_dense_rank_percent_rank_percent_rank_tier,
    approx_percentile(tier_value_rank_2, 0.5) as tier_value_rank_2_p50,
    percentile_approx(tier_value_dense_rank_2_rank, array(0.25, 0.5, 0.75)) as tier_value_dense_rank_2_rank_quartiles,
    round(avg(tier_value_dense_rank_2_rank), 2) as avg_tier_value_dense_rank_2_rank,
    approx_percentile(tier_value_dense_rank_2_rank, array(0.25, 0.5, 0.75)) as tier_value_dense_rank_2_rank_quartiles_2,
    avg(tier_value_dense_rank_percent_rank) as avg_tier_value_dense_rank_percent_rank,
    percentile_approx(tier_value_rank, 0.95) as tier_value_rank_p95
from scored
group by case when tier_value_dense_rank_percent_rank_percent_rank >= 0.69 then 'HIGH' else 'LOW' end
