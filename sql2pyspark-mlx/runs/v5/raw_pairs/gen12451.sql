SELECT id, currency, EXPLODE(discounts) AS tier_value FROM price_tiers;
