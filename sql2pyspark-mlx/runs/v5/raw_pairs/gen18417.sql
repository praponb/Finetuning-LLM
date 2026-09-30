SELECT id, created_at, coupon_codes, k, value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) e AS k, value;
