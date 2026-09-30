with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        explode(discounts) as val
    from price_tiers
)
select
    id,
    plan,
    effective_from,
    currency,
    val
from exploded
order by id desc;
