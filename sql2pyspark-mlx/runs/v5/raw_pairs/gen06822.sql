select
    c.id,
    c.customer_id,
    c.created_at,
    c.coupon_codes,
    item.product_id,
    item.qty,
    item.price,
    item.promo
from carts c
lateral view explode(c.items) e as item;
