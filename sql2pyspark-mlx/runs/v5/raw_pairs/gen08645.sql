SELECT id, plan, currency, discounts, SLICE(tier_limits, 1, 2) AS first_two_limits, ARRAY_MIN(tier_prices) AS lowest_price FROM price_tiers WHERE currency NOT IN ('THB', 'USD');
