SELECT last_restock AS event_date FROM inventory UNION ALL SELECT signup_date AS event_date FROM customers ORDER BY event_date
