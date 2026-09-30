select
    id,
    discounts,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
    element_at(tier_prices, 1) as entry_price,
    slice(tier_limits, 1, 2) as first_two_limits
from price_tiers;
