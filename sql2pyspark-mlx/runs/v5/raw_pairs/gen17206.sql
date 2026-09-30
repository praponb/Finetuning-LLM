with lines as (select id, plan, effective_from, explode(tier_limits) as element from price_tiers) select plan, effective_from from lines order by effective_from nulls last;
