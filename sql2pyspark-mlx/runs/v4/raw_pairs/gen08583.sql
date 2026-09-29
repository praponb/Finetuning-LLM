with lines as (select id, created_at, coupon_codes, attr_key, attr_value from carts lateral view explode(attributes) x as attr_key, attr_value) select id, created_at, attr_key, attr_value from lines;
