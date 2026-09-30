SELECT opened_date AS event_date FROM stores UNION SELECT delivered_date AS event_date FROM shipments ORDER BY event_date DESC;
