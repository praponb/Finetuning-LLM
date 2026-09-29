SELECT id, customer_id, coupon_codes, ship_to, ship_to.zip AS ship_zip, SIZE(items) AS num_items FROM carts WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5');
