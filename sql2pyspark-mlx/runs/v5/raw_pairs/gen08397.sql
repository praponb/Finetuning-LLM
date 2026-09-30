with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        element
    from price_tiers
    lateral view explode(discounts) e as element
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(element, 0) as element_filled
    from lines
    where plan not in ('enterprise', 'pro')
),
filtered_price_tiers as (
    -- Keep recent, valid rows
    select *, cast(element as int) as element_int
    from filtered
    where element_filled <= 35
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        element,
        element_int,
        length(plan) as plan_length,
        element_filled * element_int as element_filled_times_element_int
    from filtered_price_tiers
    where element_filled > 48
)
-- Multi-level aggregation
select
    plan,
    approx_percentile(element, array(0.25, 0.5, 0.75)) as element_quartiles,
    count_if(plan = 'enterprise') as enterprise_events,
    count(*) as row_count
from valid
group by 1
order by plan desc nulls last
