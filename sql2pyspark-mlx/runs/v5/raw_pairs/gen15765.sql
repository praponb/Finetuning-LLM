select
    id,
    coupon_codes,
    line.product_id,
    line.qty,
    line.price,
    line.promo
from carts
lateral view outer explode(items) t as line;
