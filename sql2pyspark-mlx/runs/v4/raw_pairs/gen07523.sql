select id, tier_value
from price_tiers
lateral view explode(tier_limits) e as tier_value
