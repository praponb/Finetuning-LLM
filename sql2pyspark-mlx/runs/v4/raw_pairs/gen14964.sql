select
    id,
    plan,
    effective_from,
    explode_outer(discounts) as element
from price_tiers;
