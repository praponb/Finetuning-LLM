select id, created_at, code from carts lateral view explode(coupon_codes) t as code;
