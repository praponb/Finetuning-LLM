with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        element
    from price_tiers
    lateral view outer explode(tier_limits) x as element
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(element as double) element_num
    from lines
    where element is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        element,
        element_num,
        stddev_samp(element_num) over w_ordered plan_stddev_samp_element_num,
        avg(element) over w_ordered plan_avg_element,
        percent_rank() over (partition by plan order by element) element_percent_rank
    from filtered
    where plan not in ('pro', 'starter', 'enterprise')
    window w_ordered as (partition by plan)
),
scored as (
    -- Running and moving aggregates
    select
        id,
        plan,
        plan_stddev_samp_element_num,
        plan_avg_element,
        element_percent_rank,
        sum(element_percent_rank) over (partition by plan) plan_sum_element_percent_rank,
        cume_dist() over (order by element_percent_rank desc nulls last) element_percent_rank_cume_dist
    from enriched
)
-- Multi-level aggregation
select
    plan,
    case when plan_sum_element_percent_rank >= 3 then 'PREMIUM' else 'STANDARD' end as plan_sum_element_percent_rank_tier,
    count(*) filter (where plan = 'enterprise') as enterprise_rows,
    count(case when plan = 'pro' then 1 end) as pro_count,
    percentile_approx(plan_stddev_samp_element_num, array(0.25, 0.5, 0.75)) as plan_stddev_samp_element_num_quartiles,
    count(*) filter (where plan = 'enterprise') as enterprise_rows_2,
    count(*) filter (where plan = 'pro') as pro_rows,
    grouping_id() as grouping_id
from scored
group by grouping sets ((plan, case when plan_sum_element_percent_rank >= 3 then 'PREMIUM' else 'STANDARD' end), (plan), ())
