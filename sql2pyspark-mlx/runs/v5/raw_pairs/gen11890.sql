SELECT 'shipment' AS origin, delivered_date AS event_date FROM shipments UNION SELECT 'store' AS origin, opened_date AS event_date FROM stores
