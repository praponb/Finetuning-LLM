select id, plan, effective_from, currency, val from price_tiers lateral view explode(discounts) li as val;
