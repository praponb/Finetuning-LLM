SELECT id, customer_id, coupon_codes, elem.product_id, elem.qty, elem.price, elem.promo FROM carts LATERAL VIEW OUTER EXPLODE(items) li AS elem
