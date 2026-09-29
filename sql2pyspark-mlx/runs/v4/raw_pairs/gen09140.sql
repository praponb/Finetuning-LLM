with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(tier_prices) as element
    from price_tiers
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        element,
        initcap(trim(plan)) as plan_title
    from lines
    where element between 23 and 58
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by plan order by element desc nulls first) as element_dense_rank,
        cume_dist() over (order by element desc) as element_cume_dist,
        rank() over (partition by plan order by element desc nulls last) as element_rank
    from valid
),
metrics as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by plan order by element_dense_rank nulls last) as element_dense_rank_percent_rank,
        dense_rank() over (partition by plan order by element_cume_dist) as element_cume_dist_dense_rank,
        max(element) over (partition by plan) as plan_max_element
    from with_windows
),
with_windows_price_tiers as (
    -- Running and moving aggregates
    select
        id,
        plan,
        effective_from,
        element,
        plan_title,
        element_dense_rank,
        element_cume_dist,
        element_rank,
        element_dense_rank_percent_rank,
        element_cume_dist_dense_rank,
        plan_max_element,
        cume_dist() over (partition by plan order by element_dense_rank nulls last) as element_dense_rank_cume_dist,
        cume_dist() over w_hist as element_cume_dist_2
    from metrics
    window w_hist as (partition by plan order by element desc nulls last)
),
summary as (
    -- Aggregate with conditional metrics
    select
        plan,
        sum(case when plan = 'pro' then plan_max_element else 0 end) as pro_plan_max_element,
        count_if(plan = 'basic') as basic_events,
        count(case when plan = 'pro' then 1 end) as pro_count,
        sum(element_cume_dist_2) filter (where plan = 'starter') as starter_total_element_cume_dist_2,
        count(case when plan = 'enterprise' then 1 end) as enterprise_count,
        count(distinct id) as distinct_ids
    from with_windows_price_tiers
    group by plan
)
select
    plan,
    pro_plan_max_element,
    basic_events,
    starter_total_element_cume_dist_2,
    enterprise_count,
    distinct_ids
from summary
order by distinct_ids desc nulls first
