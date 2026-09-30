with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as element
    from price_tiers
)
-- Aggregate with conditional metrics
select
    currency,
    case when element >= 66 then 'HIGH' else 'LOW' end as element_tier,
    approx_percentile(element, array(0.25, 0.5, 0.75)) as element_quartiles,
    percentile_approx(element, array(0.25, 0.5, 0.75)) as element_quartiles_2,
    round(avg(element), 2) as avg_element,
    count(*) as event_count
from flattened
group by grouping sets ((currency, case when element >= 66 then 'HIGH' else 'LOW' end), (currency));
