with exploded as (
    select
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) e as elem
)
select
    id,
    customer_id,
    coupon_codes,
    qty,
    price,
    promo,
    case when promo rlike '^(SPRING10|WELCOME5)$' then 'PRIORITY' when promo rlike '^(VIP|BUNDLE)$' then 'EXTENDED' else 'GROUP_B' end as promo_zone
from exploded
order by id;
