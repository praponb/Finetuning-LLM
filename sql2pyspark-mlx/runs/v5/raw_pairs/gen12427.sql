select
    id,
    plan,
    effective_from,
    tier_limits,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
    element_at(tier_prices, 1) as entry_price,
    slice(tier_limits, 1, 2) as first_two_limits
from price_tiers
