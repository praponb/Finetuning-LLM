with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode_outer(tier_prices) as element
    from price_tiers
)
-- Aggregate with conditional metrics
select
    plan,
    case when element >= 70 then 'LARGE' else 'SMALL' end as element_tier,
    count(case when plan = 'basic' then 1 end) as basic_count,
    percentile_approx(element, 0.5) as element_p50,
    percentile_approx(element, array(0.25, 0.5, 0.75)) as element_quartiles,
    count(distinct plan) as distinct_plans,
    count(distinct plan) as distinct_plans_2,
    sum(case when plan = 'pro' then element else 0 end) as pro_element,
    rank() over (partition by plan order by count(distinct plan) desc) as distinct_plans_rank
from flattened
group by plan, element_tier
having element_p50 < 0
order by plan nulls first;
