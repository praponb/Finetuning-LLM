SELECT id, delivered_date, status FROM shipments WHERE delivered_date < '2025-04-01' INTERSECT SELECT id, delivered_date, status FROM shipments WHERE MONTH(shipped_date) IN (1, 6, 12);
