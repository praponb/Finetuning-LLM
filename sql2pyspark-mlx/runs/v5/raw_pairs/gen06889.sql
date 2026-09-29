SELECT id, delivered_date, cost, status FROM shipments WHERE status IN ('IN_TRANSIT', 'RETURNED') AND shipped_date BETWEEN '2024-05-01' AND '2025-05-30'
