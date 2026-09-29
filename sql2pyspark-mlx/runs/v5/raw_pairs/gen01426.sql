with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        explode_outer(tier_limits) as element
    from price_tiers
)
-- Multi-level aggregation
select
    date_trunc('quarter', effective_from) as quarter_start,
    date_trunc('year', effective_from) as year_start,
    percentile_approx(element, 0.9) as element_p90,
    count(*) as num_rows,
    round(avg(element), 2) as avg_element
from flattened
group by date_trunc('quarter', effective_from), date_trunc('year', effective_from)
having count(*) > 1
