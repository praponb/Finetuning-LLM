with lines as (
    select
        t.id,
        t.created_at,
        t.coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    from carts t
    lateral view explode(t.items) x as li_item
),
filtered as (
    select *, size(coupon_codes) as coupon_codes_count
    from lines
    where created_at >= date '2025-03-01'
      and promo is not distinct from 'WELCOME5'
),
cleaned as (
    select
        *,
        size(coupon_codes) as coupon_codes_count_2,
        coalesce(promo, 'UNKNOWN') as resolved_promo
    from filtered
    where created_at > timestamp '2025-03-01 16:00:00'
      and promo is distinct from 'VIP'
),
valid as (
    select *, qty - coupon_codes_count_2 as qty_minus_coupon_codes_count_2
    from cleaned
    where promo is distinct from 'WELCOME5'
)
select
    created_at,
    price,
    coupon_codes_count,
    coupon_codes_count_2,
    qty_minus_coupon_codes_count_2
from valid
