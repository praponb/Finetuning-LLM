with flattened as (
    select
        id,
        plan,
        explode_outer(discounts) as tier_value
    from price_tiers
)
select id, plan
from flattened
order by id desc nulls first;
