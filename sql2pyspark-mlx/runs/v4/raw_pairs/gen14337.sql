select
    id,
    plan,
    currency,
    tier_limits,
    element_at(tier_prices, 1) as entry_price,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices
from price_tiers
where plan != 'basic'
