with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) li as key, value
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        value,
        code
    from flattened
    lateral view outer explode(coupon_codes) x as code
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        upper(code) code_upper,
        cast(created_at as date) created_at_day
    from exploded
    where created_at >= to_date('2025-03-01')
      and code is distinct from 'WELCOME5'
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        created_at,
        value,
        code,
        code_upper,
        cast(created_at as date) created_at_day_2,
        coalesce(code, 'N/A') code_clean
    from base
    where created_at_day > '2024-07-01'
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        created_at,
        value,
        code_upper,
        code_clean,
        trim(code) clean_code,
        last_day(created_at_day_2) created_at_day_2_month_end
    from cleaned
    where code is not null
      and code is distinct from 'VIP'
)
-- Grouped metrics
select
    customer_id,
    count(distinct id) as distinct_ids,
    min(created_at) as started_at
from filtered
group by customer_id
order by customer_id
