SELECT delivered_date, carrier, cost, LOWER(status) AS status_lower FROM shipments WHERE shipped_date BETWEEN '2024-03-01' AND '2025-04-03' AND order_id IS NOT NULL
