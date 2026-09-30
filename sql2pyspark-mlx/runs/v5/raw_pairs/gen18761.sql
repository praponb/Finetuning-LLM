SELECT id, store_id, shipping_fee, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS rn FROM orders
