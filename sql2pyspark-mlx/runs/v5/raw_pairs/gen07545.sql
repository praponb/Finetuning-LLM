select id, customer_id, coupon_codes, attr_key, value from carts lateral view explode(attributes) t as attr_key, value
