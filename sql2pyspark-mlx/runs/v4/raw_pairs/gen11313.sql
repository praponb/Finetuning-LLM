with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        tier_limits,
        tier_prices,
        discounts,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
-- Aggregate with conditional metrics
select
    case when entry_price >= 24 then 'HIGH' else 'LOW' end as entry_price_tier,
    plan,
    count_if(plan = 'starter') as starter_events,
    round(sum(avg_tier_price) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as avg_tier_price_per_enterprise,
    count(case when plan = 'basic' then 1 end) as basic_count,
    sum(case when plan = 'basic' then entry_price else 0 end) as basic_entry_price,
    count(distinct id) as distinct_ids,
    round(sum(avg_tier_price) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as avg_tier_price_per_enterprise_2
from flattened_attrs
group by cube (case when entry_price >= 24 then 'HIGH' else 'LOW' end, plan)
having round(sum(avg_tier_price) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) < 0
order by plan desc
