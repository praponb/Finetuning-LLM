with flattened as (select id, plan, effective_from, currency, element from price_tiers lateral view explode(discounts) t as element) select id, element from flattened;
