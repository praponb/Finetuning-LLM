with flattened as (
    -- Unnest the array into one row per element
    select id, value
    from carts
    lateral view explode(coupon_codes) t as value
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(value, 'N/A') value_filled,
        coalesce(value, 'DIRECT') resolved_value
    from flattened
    where value in ('BUNDLE', 'SPRING10')
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(value, 'N/A') value_filled_2,
        lower(value) value_lower,
        coalesce(value, 'N/A') value_filled_3
    from valid
    where value = 'BUNDLE'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, trim(value) clean_value
    from valid_carts
    where value <> 'WELCOME5'
)
select
    id,
    value,
    value_filled,
    resolved_value,
    case when value rlike '^(WELCOME5|VIP)$' then 'CORE' when value rlike '^(SPRING10|BUNDLE)$' then 'PRIORITY' else 'OTHER_ZONE' end as value_zone
from cleaned
order by value_zone nulls first;
