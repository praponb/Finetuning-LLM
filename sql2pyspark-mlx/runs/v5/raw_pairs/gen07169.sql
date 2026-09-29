SELECT * FROM orders WHERE store_id = 36 AND store_id IN (17, 31, 40) ORDER BY shipping_fee DESC, id LIMIT 10
