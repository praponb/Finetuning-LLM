SELECT id, customer_id, created_at, coupon_codes, k, v FROM carts LATERAL VIEW EXPLODE(attributes) li AS k, v;
