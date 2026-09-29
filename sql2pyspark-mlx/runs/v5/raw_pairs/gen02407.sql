with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        element
    from price_tiers
    lateral view outer explode(tier_limits) e as element
),
with_windows as (
    -- Window metrics over several different partitions
    select *, min(element) over (partition by plan) as plan_min_element
    from lines
),
base as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(plan)) as plan_title,
        case when plan in ('starter', 'basic') then 'Tier 1' else 'Other' end as plan_group
    from with_windows
    where element <= 20
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        element,
        plan_min_element,
        plan_title,
        plan_group,
        plan_min_element - element as plan_min_element_minus_element
    from base
    where plan_min_element between 31 and 58
)
-- Aggregate with conditional metrics
select
    case when plan_min_element >= 82 then 'PREMIUM' else 'STANDARD' end as plan_min_element_tier,
    case when plan_min_element_minus_element >= 61 then 'PREMIUM' else 'STANDARD' end as plan_min_element_minus_element_tier,
    approx_percentile(plan_min_element, array(0.25, 0.5, 0.75)) as plan_min_element_quartiles,
    count(*) as num_rows,
    count(*) as num_rows_2,
    grouping(case when plan_min_element >= 82 then 'PREMIUM' else 'STANDARD' end) as grouping_level
from valid
group by rollup (case when plan_min_element >= 82 then 'PREMIUM' else 'STANDARD' end, case when plan_min_element_minus_element >= 61 then 'PREMIUM' else 'STANDARD' end)
having num_rows > 1
order by plan_min_element_minus_element_tier desc nulls first
