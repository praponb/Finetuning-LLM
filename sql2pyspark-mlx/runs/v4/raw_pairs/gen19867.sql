SELECT order_date, 'order' AS source FROM orders WHERE customer_id IS NOT NULL UNION ALL SELECT last_restock, 'inventory' AS source FROM inventory WHERE store_id = 3
