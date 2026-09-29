select
    id,
    plan,
    element
from price_tiers
lateral view outer explode(tier_limits) t as element
