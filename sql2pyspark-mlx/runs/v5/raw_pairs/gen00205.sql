with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) e as key, value
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        value,
        code
    from exploded
    lateral view outer explode(coupon_codes) li as code
),
matched_keys as (
    -- Set intersection on keys
    select id
    from lines
    intersect all
    select id
    from carts
    where hour(created_at) between 8 and 11
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from lines s
    inner join matched_keys k on s.id = k.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        lower(code) as code_lower,
        hour(created_at) as created_at_hour,
        coalesce(code, 'OTHER') as resolved_code
    from kept_rows
    where code is not null
      and created_at >= '2023-10-08'
),
filtered_carts as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), created_at) as days_since_created_at,
        coalesce(code, 'N/A') as code_clean
    from filtered
    where code is not distinct from 'VIP'
      and created_at_hour <> 47
)
select
    id,
    code,
    code_lower,
    created_at_hour,
    resolved_code,
    days_since_created_at,
    code_clean,
    case when code rlike '^(BUNDLE|SPRING10)$' then 'CORE' when code rlike '^(VIP|WELCOME5)$' then 'EXTENDED' else 'PRIORITY' end as code_zone
from filtered_carts
