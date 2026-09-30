SELECT id, customer_id, created_at, coupon_codes, pos + 1 AS item_no, elem.product_id, elem.qty FROM carts LATERAL VIEW POSEXPLODE(items) x AS pos, elem
