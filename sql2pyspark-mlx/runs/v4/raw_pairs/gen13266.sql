SELECT status FROM orders WHERE order_date >= '2024-04-01' UNION ALL SELECT status FROM shipments ORDER BY status
