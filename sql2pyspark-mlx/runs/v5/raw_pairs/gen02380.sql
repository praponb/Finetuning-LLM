with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        element
    from price_tiers
    lateral view explode(tier_limits) t as element
)
-- Aggregate with conditional metrics
select
    case when element >= 74 then 'PREMIUM' else 'STANDARD' end as element_tier,
    date_trunc('quarter', effective_from) as quarter_start,
    sum(element) as total_element,
    sum(element) as total_element_2
from flattened
group by case when element >= 74 then 'PREMIUM' else 'STANDARD' end, date_trunc('quarter', effective_from)
order by element_tier nulls last
