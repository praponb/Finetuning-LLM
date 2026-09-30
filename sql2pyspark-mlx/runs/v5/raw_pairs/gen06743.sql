select id, explode_outer(discounts) as val from price_tiers;
