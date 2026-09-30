select id, customer_id, coupon_codes, line.product_id, line.promo from carts lateral view outer explode(items) li as line;
