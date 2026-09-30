with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_limits) as element
    from price_tiers
)
-- Multi-level aggregation
select
    date_trunc('quarter', effective_from) as quarter_start,
    currency,
    percentile_approx(element, array(0.25, 0.5, 0.75)) as element_quartiles,
    count(distinct id) as distinct_ids,
    size(collect_set(currency)) as num_currencies,
    grouping_id() as grouping_id
from lines
group by cube (date_trunc('quarter', effective_from), currency)
