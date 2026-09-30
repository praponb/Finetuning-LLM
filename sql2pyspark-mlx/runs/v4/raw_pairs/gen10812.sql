with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode_outer(discounts) as tier_value
    from price_tiers
),
scored as (
    -- Running and moving aggregates
    select
        *,
        count(tier_value) over w_hist plan_count_tier_value,
        percent_rank() over w_ordered tier_value_percent_rank,
        cume_dist() over w_ordered tier_value_cume_dist
    from flattened
    window w_hist as (partition by plan), w_ordered as (partition by plan order by tier_value desc nulls last)
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(tier_value_cume_dist, 2) tier_value_cume_dist_rounded
    from scored
    where tier_value_percent_rank <= 0.42
),
enriched as (
    -- Ranking and gap features
    select
        *,
        max(tier_value_cume_dist_rounded) over (partition by plan) plan_max_tier_value_cume_dist_rounded,
        cume_dist() over (order by tier_value_cume_dist nulls last) tier_value_cume_dist_cume_dist,
        dense_rank() over (partition by plan order by tier_value_cume_dist desc nulls first) tier_value_cume_dist_dense_rank,
        max(plan_count_tier_value) over (partition by plan) plan_max_plan_count_tier_value
    from cleaned
)
select
    id,
    tier_value_cume_dist,
    tier_value_cume_dist_rounded,
    plan_max_tier_value_cume_dist_rounded,
    tier_value_cume_dist_cume_dist
from enriched
order by tier_value_cume_dist desc nulls first, tier_value_cume_dist_rounded desc
