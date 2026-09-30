select id, created_at, coupon_codes, key, value from carts lateral view outer explode(attributes) x as key, value
