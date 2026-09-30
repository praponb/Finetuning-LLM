with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as element
    from price_tiers
)
select
    id,
    plan,
    currency,
    element
from exploded
order by currency desc;
