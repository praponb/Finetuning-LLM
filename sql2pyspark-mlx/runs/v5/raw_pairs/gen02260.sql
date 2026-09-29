with exploded as (
    select
        id,
        plan,
        element
    from price_tiers
    lateral view explode(discounts) li as element
),
filtered as (
    select *, floor(element) as element_whole
    from exploded
    where plan <> 'basic'
),
cleaned as (
    select *, element_whole - element as element_whole_minus_element
    from filtered
    where element > 80
),
enriched as (
    select
        *,
        max(element_whole_minus_element) over (partition by plan) as plan_max_element_whole_minus_element,
        avg(element_whole) over (partition by plan) as plan_avg_element_whole,
        sum(element_whole) over (partition by plan) as plan_sum_element_whole,
        dense_rank() over (order by element desc) as element_dense_rank,
        rank() over (partition by plan order by element_whole) as element_whole_rank
    from cleaned
    where element_whole_minus_element >= 53
),
with_windows as (
    select
        *,
        percent_rank() over (order by element_whole_rank desc nulls first) as element_whole_rank_percent_rank,
        stddev_samp(plan_avg_element_whole) over (partition by plan) as plan_stddev_samp_plan_avg_element_whole,
        cume_dist() over (partition by plan order by plan_avg_element_whole desc nulls last) as plan_avg_element_whole_cume_dist
    from enriched
),
scored as (
    select
        *,
        rank() over (order by element) as element_rank,
        percent_rank() over (partition by plan order by element_whole_rank_percent_rank desc) as element_whole_rank_percent_rank_percent_rank
    from with_windows
)
select
    id,
    plan,
    element,
    element_whole,
    plan_max_element_whole_minus_element,
    plan_avg_element_whole,
    plan_sum_element_whole,
    element_dense_rank,
    plan_stddev_samp_plan_avg_element_whole
from scored
order by id;
