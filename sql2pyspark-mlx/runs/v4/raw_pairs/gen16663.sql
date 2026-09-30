with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode(discounts) as element
    from price_tiers
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        date_format(effective_from, 'yyyy-MM-dd') as effective_from_label,
        length(plan) as plan_length
    from flattened
    where effective_from >= date '2024-10-03'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        plan,
        effective_from,
        element,
        effective_from_label,
        plan_length,
        cume_dist() over (partition by plan order by plan_length nulls last) as plan_length_cume_dist,
        rank() over (partition by plan order by element desc nulls last) as element_rank,
        min(plan_length) over (partition by plan) as plan_min_plan_length
    from cleaned
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by plan order by plan_length nulls first) as plan_length_dense_rank,
        cume_dist() over (partition by plan order by element desc) as element_cume_dist
    from enriched
),
totals as (
    -- Multi-level aggregation
    select
        case when plan_length_dense_rank >= 8 then 'HIGH' else 'LOW' end as plan_length_dense_rank_tier,
        case when plan_min_plan_length >= 74 then 'HIGH' else 'LOW' end as plan_min_plan_length_tier,
        count(*) as cnt,
        sort_array(collect_set(plan)) as plan_list,
        percentile_approx(element_cume_dist, array(0.25, 0.5, 0.75)) as element_cume_dist_quartiles,
        round(sum(element) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as element_per_enterprise
    from with_windows
    group by case when plan_length_dense_rank >= 8 then 'HIGH' else 'LOW' end, case when plan_min_plan_length >= 74 then 'HIGH' else 'LOW' end
    having cnt > 1
)
select
    plan_length_dense_rank_tier,
    plan_min_plan_length_tier,
    cnt,
    element_cume_dist_quartiles
from totals
order by plan_min_plan_length_tier nulls first;
