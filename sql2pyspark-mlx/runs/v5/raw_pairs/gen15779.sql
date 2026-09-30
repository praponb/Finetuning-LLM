select
    id,
    plan,
    effective_from,
    currency,
    tier_prices,
    discounts,
    slice(tier_limits, 1, 2) as first_two_limits,
    element_at(tier_prices, 1) as entry_price
from price_tiers
