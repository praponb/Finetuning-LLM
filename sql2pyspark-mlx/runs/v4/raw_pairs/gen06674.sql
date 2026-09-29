select
    id,
    plan,
    currency,
    explode(discounts) as val
from price_tiers
