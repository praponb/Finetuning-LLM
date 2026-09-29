select
    id,
    plan,
    effective_from,
    currency,
    val
from price_tiers
lateral view explode(discounts) x as val
