SELECT id, created_at, coupon_codes, line.product_id, line.price FROM carts LATERAL VIEW EXPLODE(items) x AS line;
