WITH lines AS (SELECT id, created_at, value FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value) SELECT id FROM lines
