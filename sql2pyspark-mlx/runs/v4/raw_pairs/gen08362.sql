SELECT id, customer_id, created_at, coupon_codes, attr_key, value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value;
