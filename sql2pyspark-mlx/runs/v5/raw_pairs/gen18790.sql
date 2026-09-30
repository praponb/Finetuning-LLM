select id, plan, effective_from, currency, explode_outer(discounts) as val from price_tiers;
