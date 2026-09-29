SELECT c.id, c.customer_id, c.created_at, c.coupon_codes, line.product_id, line.price, line.promo FROM carts c LATERAL VIEW OUTER EXPLODE(c.items) x AS line;
