SELECT 'order' AS origin, order_date AS event_date FROM orders UNION SELECT 'inventory' AS origin, last_restock AS event_date FROM inventory
