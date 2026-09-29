select id, customer_id, coupon_codes, k, attr_value from carts lateral view explode(attributes) li as k, attr_value;
