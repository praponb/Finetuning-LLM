SELECT id, customer_id, coupon_codes, key, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) li AS key, attr_value;
