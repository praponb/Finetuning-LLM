SELECT launch_date AS event_date FROM products UNION SELECT last_restock AS event_date FROM inventory ORDER BY event_date DESC
