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
    id,
    plan,
    stddev_samp(element) over w_recent as plan_stddev_samp_element,
    percent_rank() over (partition by plan order by element desc nulls first) as element_percent_rank,
    cume_dist() over (partition by currency, plan order by element desc nulls first) as element_cume_dist
from lines
where plan in ('enterprise', 'starter', 'basic')
window w_recent as (partition by plan);
