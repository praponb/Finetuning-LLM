select
    id,
    effective_from,
    explode(tier_limits) as element
from price_tiers
