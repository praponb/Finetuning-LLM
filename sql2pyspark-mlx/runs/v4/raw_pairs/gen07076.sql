SELECT id, customer_id, created_at, coupon_codes, pos, li_item.product_id, li_item.promo FROM carts LATERAL VIEW POSEXPLODE(items) x AS pos, li_item
