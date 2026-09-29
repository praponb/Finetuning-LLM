select id, plan, effective_from, currency, explode_outer(discounts) as tier_value from price_tiers;
