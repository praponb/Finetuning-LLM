SELECT id, ship_to, ship_to.city AS ship_city, TRANSFORM(items, x -> x.product_id) AS product_ids, SIZE(items) AS num_items, ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text FROM carts
