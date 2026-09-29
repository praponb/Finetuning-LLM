with flattened as (
    -- Unnest the array into one row per element
    select id, element
    from price_tiers
    lateral view explode(tier_prices) x as element
),
valid as (
    -- Basic cleansing and defaults
    select *, round(element * 100, 2) as adjusted_element
    from flattened
    where element < 68
),
valid_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select *, adjusted_element + element as adjusted_element_plus_element
    from valid
    where adjusted_element <= 40
),
valid_price_tiers_2 as (
    -- Filter the raw rows and derive helper columns
    select *, cast(adjusted_element as int) as adjusted_element_int
    from valid_price_tiers
    where adjusted_element between 58 and 59
)
-- Multi-level aggregation
select
    case when adjusted_element_plus_element >= 78 then 'HIGH' else 'LOW' end as adjusted_element_plus_element_tier,
    case when adjusted_element >= 64 then 'PREMIUM' else 'STANDARD' end as adjusted_element_tier,
    count(*) as event_count,
    percentile_approx(element, array(0.25, 0.5, 0.75)) as element_quartiles
from valid_price_tiers_2
group by case when adjusted_element_plus_element >= 78 then 'HIGH' else 'LOW' end, case when adjusted_element >= 64 then 'PREMIUM' else 'STANDARD' end
order by event_count desc
