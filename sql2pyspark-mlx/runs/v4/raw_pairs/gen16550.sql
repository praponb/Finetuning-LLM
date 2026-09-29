SELECT id, status FROM orders WHERE order_date >= '2024-07-01' UNION ALL SELECT id, status FROM orders WHERE status = 'COMPLETED' ORDER BY id;
