select id, explode(tier_limits) as tier_value from price_tiers;
