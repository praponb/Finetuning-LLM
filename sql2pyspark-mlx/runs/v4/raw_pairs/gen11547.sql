select
    id,
    effective_from,
    currency,
    explode_outer(tier_prices) as element
from price_tiers;
