select id, code from carts lateral view outer explode(coupon_codes) e as code;
