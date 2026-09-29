SELECT id, customer_id, created_at, coupon_codes, ship_to, ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price, EXISTS(items, li -> li.price > 100) AS has_premium_item FROM carts;
