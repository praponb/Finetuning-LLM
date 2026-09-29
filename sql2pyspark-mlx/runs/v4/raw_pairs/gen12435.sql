with flattened as (
    -- Unnest the array into one row per element
    select id, explode_outer(coupon_codes) as element
    from carts
),
base as (
    -- Keep recent, valid rows
    select
        *,
        lower(element) as element_lower,
        substring(element, 1, 3) as element_prefix,
        coalesce(element, 'NONE') as resolved_element
    from flattened
    where element = 'BUNDLE'
),
base_carts as (
    -- Basic cleansing and defaults
    select
        id,
        element,
        element_lower,
        element_prefix,
        resolved_element,
        coalesce(element, 'unassigned') as element_filled,
        initcap(trim(element)) as element_title,
        coalesce(element, 'UNKNOWN') as element_clean
    from base
    where element is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        substring(element, 1, 3) as element_prefix_2,
        substring(element, 1, 4) as element_prefix_3
    from base_carts
    where element in ('WELCOME5', 'SPRING10', 'BUNDLE')
),
base_carts_2 as (
    -- Keep recent, valid rows
    select
        *,
        length(element) as element_length,
        coalesce(element, 'NONE') as element_clean_2
    from filtered
    where element is not null
),
stats as (
    -- Aggregate with conditional metrics
    select
        case when element_length >= 87 then 'PREMIUM' else 'STANDARD' end as element_length_tier,
        count_if(element = 'WELCOME5') as welcome5_events,
        count(*) filter (where element = 'WELCOME5') as welcome5_rows,
        min(element_length) as min_element_length,
        count(*) as row_count,
        sum(element_length) filter (where element = 'WELCOME5') as welcome5_total_element_length,
        sum(element_length) as total_element_length
    from base_carts_2
    group by case when element_length >= 87 then 'PREMIUM' else 'STANDARD' end
    having count(*) < 1
)
select
    element_length_tier,
    welcome5_events,
    welcome5_rows,
    min_element_length,
    total_element_length
from stats
order by welcome5_events desc nulls first, total_element_length;
