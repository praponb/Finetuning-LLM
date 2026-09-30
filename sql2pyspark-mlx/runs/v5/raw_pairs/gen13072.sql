SELECT sale_date AS event_date FROM store_sales WHERE store_id = 5 UNION ALL SELECT last_restock AS event_date FROM inventory WHERE store_id IN (2, 4, 5) ORDER BY event_date;
