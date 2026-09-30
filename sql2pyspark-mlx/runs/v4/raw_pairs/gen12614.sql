with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        element
    from price_tiers
    lateral view outer explode(tier_limits) x as element
)
-- Running and moving aggregates
select
    id,
    effective_from,
    element,
    rank() over (order by element desc) as element_rank,
    cume_dist() over (order by element nulls first) as element_cume_dist,
    rank() over (order by element) as element_rank_2,
    cume_dist() over (order by element desc nulls first) as element_cume_dist_2
from exploded;
