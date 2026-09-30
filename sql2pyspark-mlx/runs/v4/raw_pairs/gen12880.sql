select
    id,
    customer_id,
    created_at,
    element
from carts
lateral view outer explode(coupon_codes) e as element;
