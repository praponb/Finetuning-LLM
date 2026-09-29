SELECT id, currency, tier_prices, ARRAY_MIN(tier_prices) AS lowest_price, ELEMENT_AT(tier_prices, 1) AS entry_price FROM price_tiers
