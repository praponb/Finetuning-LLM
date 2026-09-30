with flattened as (
    select
        id,
        currency,
        explode_outer(tier_limits) as element
    from price_tiers
)
select id, element
from flattened;
