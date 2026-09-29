with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    from carts
    lateral view outer explode(items) t as elem
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        product_id,
        explode_outer(coupon_codes) as element
    from exploded
),
enriched as (
    -- Attach customers attributes
    select s.*, c.country
    from lines s
    left join customers c
        on s.customer_id = c.id
    where not c.is_active
),
base as (
    -- Keep recent, valid rows
    select
        customer_id,
        created_at,
        product_id,
        element,
        country,
        upper(element) as element_upper,
        hour(created_at) as created_at_hour
    from enriched
    where country is distinct from 'JP'
      and created_at > timestamp '2025-03-01 13:00:00'
)
-- Multi-level aggregation
select
    date_trunc('day', created_at) as activity_day,
    customer_id,
    min(created_at) as first_created_at,
    min(created_at) as first_seen,
    round(avg(product_id), 2) as avg_product_id,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min,
    count(*) filter (where element = 'VIP') as vip_rows,
    min(created_at) as first_created_at_2
from base
group by grouping sets ((date_trunc('day', created_at), customer_id), (date_trunc('day', created_at)))
