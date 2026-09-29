SELECT id, shipped_date, status, CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END AS is_in_transit FROM shipments WHERE shipped_date < '2025-01-19' AND cost < 43
