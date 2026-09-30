SELECT id, status, delivered_date, shipped_date FROM shipments WHERE delivered_date BETWEEN '2024-10-01' AND '2025-01-01' OR status = 'LOST'
