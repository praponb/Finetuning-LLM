select
    id,
    effective_from,
    explode_outer(tier_prices) as val
from price_tiers
