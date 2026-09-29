SELECT opened_date AS event_date FROM stores UNION ALL SELECT shipped_date AS event_date FROM shipments ORDER BY event_date DESC;
