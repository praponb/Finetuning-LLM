select
    id,
    coupon_codes,
    key,
    value
from carts
lateral view outer explode(attributes) t as key, value;
