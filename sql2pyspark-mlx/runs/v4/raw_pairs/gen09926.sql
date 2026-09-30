SELECT order_id, status FROM shipments EXCEPT SELECT order_id, status FROM shipments WHERE shipped_date <= '2025-06-01'
