SELECT id, customer_id, created_at, coupon_codes, line.price, line.promo FROM carts LATERAL VIEW EXPLODE(items) t AS line
