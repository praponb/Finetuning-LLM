with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        tier_limits,
        element_at(tier_prices, 1) as entry_price,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
)
-- Running and moving aggregates
select
    id,
    plan,
    tier_limits,
    entry_price,
    net_prices,
    avg_tier_price,
    tier_revenue_caps,
    avg(entry_price) over (partition by plan) plan_avg_entry_price,
    sum(entry_price) over (partition by plan) plan_sum_entry_price,
    first_value(avg_tier_price) ignore nulls over (order by avg_tier_price asc, id asc rows between unbounded preceding and current row) first_known_avg_tier_price
from unpacked
