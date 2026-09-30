SELECT id, customer_id, created_at, coupon_codes, entry.product_id, entry.qty, entry.price FROM carts LATERAL VIEW EXPLODE(items) t AS entry;
