with exploded as (
    -- Unnest the array into one row per element
    select
        c.id,
        c.created_at,
        c.coupon_codes,
        pos,
        item.qty,
        item.price,
        item.promo
    from carts c
    lateral view outer posexplode(c.items) x as pos, item
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
    -- Basic cleansing and defaults
    select *, coalesce(code, 'N/A') code_filled
    from lines
    where created_at > timestamp '2025-03-01 18:00:00'
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        code,
        code_filled,
        substring(code, 1, 2) code_prefix
    from filtered
    where created_at > timestamp '2025-03-01 23:00:00'
),
base as (
    -- Basic cleansing and defaults
    select *, lower(code) code_lower
    from cleaned
    where code is not null
      and code is distinct from 'WELCOME5'
)
select
    id,
    code,
    code_filled,
    code_prefix,
    code_lower
from base
order by code desc nulls first, id desc;
