WITH flattened AS (SELECT id, created_at, coupon_codes, k, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v) SELECT id, created_at, coupon_codes, k, v FROM flattened ORDER BY k
