SELECT id, customer_id, element FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
