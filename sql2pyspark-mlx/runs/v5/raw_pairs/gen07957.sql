with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.promo
    from carts
    lateral view outer explode(items) x as elem
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        promo,
        explode_outer(coupon_codes) as code
    from exploded
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_format(created_at, 'MMM yyyy') as created_at_label,
        coalesce(promo, 'OTHER') as promo_filled
    from lines
    where code is not distinct from 'VIP'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        created_at,
        code,
        created_at_label,
        promo_filled,
        to_date(created_at) as created_at_date,
        coalesce(code, 'DIRECT') as code_filled
    from filtered
    where created_at >= timestamp '2025-03-01 22:00:00'
      and created_at >= '2023-04-17'
)
select
    code,
    promo_filled,
    code_filled,
    case when code rlike '^(WELCOME5|BUNDLE)$' then 'CORE' else 'PRIORITY' end as code_zone
from cleaned
order by code_zone, code;
