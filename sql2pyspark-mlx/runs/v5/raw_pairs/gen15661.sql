SELECT shipped_date AS event_date FROM shipments EXCEPT SELECT last_restock AS event_date FROM inventory ORDER BY event_date
