select id, created_at, value from carts lateral view outer explode(coupon_codes) x as value
