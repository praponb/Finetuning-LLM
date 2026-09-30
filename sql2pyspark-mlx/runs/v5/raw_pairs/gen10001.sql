with flattened_attrs as (
    select
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        discounts,
        slice(tier_limits, 1, 2) as first_two_limits,
        array_min(tier_prices) as lowest_price
    from price_tiers
)
select
    case when lowest_price >= 34 then 'PREMIUM' else 'STANDARD' end as lowest_price_tier,
    plan,
    count(distinct plan) as distinct_plans,
    round(sum(lowest_price) / nullif(count(case when plan = 'starter' then 1 end), 0), 2) as lowest_price_per_starter
from flattened_attrs
group by case when lowest_price >= 34 then 'PREMIUM' else 'STANDARD' end, plan;
