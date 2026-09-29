SELECT id, order_id, status, delivered_date, NTILE(2) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id) AS quartile FROM shipments
