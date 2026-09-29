SELECT id, shipped_date, SUBSTRING(status, 1, 4) AS status_prefix
FROM shipments
ORDER BY shipped_date DESC, status_prefix ASC
