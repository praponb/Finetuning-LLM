SELECT id, customer_id, created_at, value FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS value
