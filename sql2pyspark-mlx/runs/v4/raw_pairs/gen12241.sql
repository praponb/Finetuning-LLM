with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.promo
    from carts
    lateral view outer explode(items) x as item
)
select
    id,
    created_at,
    coupon_codes,
    product_id
from flattened
order by created_at nulls first;
