select
    id,
    plan,
    tier_limits,
    discounts,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
    aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
    element_at(tier_prices, 1) as entry_price,
    array_min(tier_prices) as lowest_price
from price_tiers;
