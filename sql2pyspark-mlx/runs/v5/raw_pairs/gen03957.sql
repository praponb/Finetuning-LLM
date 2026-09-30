SELECT id, customer_id, created_at, coupon_codes, attr_key, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, attr_value;
