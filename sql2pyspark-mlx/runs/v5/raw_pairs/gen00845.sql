with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as val
    from price_tiers
)
select
    plan,
    effective_from,
    currency
from lines
order by effective_from nulls first;
