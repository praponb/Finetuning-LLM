select id, customer_id, explode(coupon_codes) as value from carts;
