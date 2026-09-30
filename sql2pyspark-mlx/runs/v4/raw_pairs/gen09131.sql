SELECT id, channel FROM orders WHERE shipping_fee IS NULL UNION SELECT id, channel FROM orders WHERE store_id IN (15, 19, 28, 37) ORDER BY id DESC
