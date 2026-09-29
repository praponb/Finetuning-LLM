select
    id,
    created_at,
    coupon_codes,
    key,
    v
from carts
lateral view explode(attributes) li as key, v;
