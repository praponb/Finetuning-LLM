with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        explode(discounts) as tier_value
    from price_tiers
)
select
    id,
    effective_from,
    tier_value
from exploded
