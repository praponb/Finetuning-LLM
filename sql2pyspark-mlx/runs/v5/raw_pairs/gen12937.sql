with lines as (
    select
        id,
        plan,
        effective_from,
        explode(discounts) as val
    from price_tiers
)
select id, effective_from
from lines
order by effective_from nulls last, id;
