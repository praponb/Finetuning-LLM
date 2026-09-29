with exploded as (select id, effective_from, explode(tier_limits) as element from price_tiers) select id, effective_from, element from exploded
