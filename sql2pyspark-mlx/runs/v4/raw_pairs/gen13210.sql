with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        array_min(tier_prices) as lowest_price,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price
    from price_tiers
)
-- Multi-level aggregation
select
    plan,
    case when lowest_price >= 30 then 'HIGH' else 'LOW' end as lowest_price_tier,
    count_if(plan = 'enterprise') as enterprise_events,
    approx_percentile(avg_tier_price, 0.95) as avg_tier_price_p95,
    min(avg_tier_price) as min_avg_tier_price
from flattened_attrs
group by plan, case when lowest_price >= 30 then 'HIGH' else 'LOW' end
order by avg_tier_price_p95 desc nulls last
