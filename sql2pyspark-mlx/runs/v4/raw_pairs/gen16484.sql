SELECT id, created_at, coupon_codes, elem.product_id, elem.qty, elem.price FROM carts LATERAL VIEW OUTER EXPLODE(items) x AS elem
