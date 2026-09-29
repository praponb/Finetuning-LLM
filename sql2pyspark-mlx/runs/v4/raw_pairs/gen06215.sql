select id, created_at, coupon_codes, k, value from carts lateral view explode(attributes) e as k, value;
