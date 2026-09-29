with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(tier_prices) as element
    from price_tiers
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (order by element desc nulls first) as element_rank,
        count(element) over (partition by plan) as plan_count_element,
        cume_dist() over (partition by plan order by element) as element_cume_dist
    from lines
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, cast(element_rank as string) as element_rank_num
    from enriched
    where year(effective_from) >= 2024
),
scored as (
    -- Running and moving aggregates
    select
        id,
        plan,
        element,
        plan_count_element,
        element_cume_dist,
        cume_dist() over (order by element_rank desc nulls last) as element_rank_cume_dist,
        percent_rank() over (partition by plan order by element_cume_dist desc nulls last) as element_cume_dist_percent_rank
    from valid
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        element,
        plan_count_element,
        element_cume_dist_percent_rank,
        element_rank_cume_dist - element as element_rank_cume_dist_minus_element,
        cast(element as int) as element_int
    from scored
    where element_cume_dist between 0.23 and 0.29
)
-- Multi-level aggregation
select
    case when plan_count_element >= 14 then 'HIGH' else 'LOW' end as plan_count_element_tier,
    case when element_rank_cume_dist_minus_element >= 72 then 'LARGE' else 'SMALL' end as element_rank_cume_dist_minus_element_tier,
    case when element_cume_dist_percent_rank >= 0.71 then 'PREMIUM' else 'STANDARD' end as element_cume_dist_percent_rank_tier,
    count(distinct id) as distinct_ids,
    percentile_approx(plan_count_element, array(0.25, 0.5, 0.75)) as plan_count_element_quartiles,
    count(*) as row_count,
    rank() over (partition by case when plan_count_element >= 14 then 'HIGH' else 'LOW' end order by count(distinct id) desc) as distinct_ids_rank
from base
group by case when plan_count_element >= 14 then 'HIGH' else 'LOW' end, case when element_rank_cume_dist_minus_element >= 72 then 'LARGE' else 'SMALL' end, case when element_cume_dist_percent_rank >= 0.71 then 'PREMIUM' else 'STANDARD' end
order by plan_count_element_tier
