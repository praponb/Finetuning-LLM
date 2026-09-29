SELECT id, customer_id, created_at, element FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
