select
    id,
    plan,
    element
from price_tiers
lateral view explode(tier_limits) x as element;
