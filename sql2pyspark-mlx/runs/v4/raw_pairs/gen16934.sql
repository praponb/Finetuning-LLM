select id, customer_id, created_at, coupon_codes, k, value from carts lateral view explode(attributes) li as k, value
