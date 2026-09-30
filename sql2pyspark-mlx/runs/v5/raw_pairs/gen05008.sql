with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(tier_prices) as element
    from price_tiers
)
-- Running and moving aggregates
select
    plan,
    element,
    rank() over (order by element desc nulls first) as element_rank
from lines
