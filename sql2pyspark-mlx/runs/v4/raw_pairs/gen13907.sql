SELECT id, carrier, shipped_date, cost FROM shipments WHERE delivered_date BETWEEN '2023-05-06' AND '2023-10-01' AND cost <= 92 AND order_id IN (7, 39) ORDER BY shipped_date ASC
