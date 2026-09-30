SELECT id, customer_id, coupon_codes, key, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS key, attr_value
