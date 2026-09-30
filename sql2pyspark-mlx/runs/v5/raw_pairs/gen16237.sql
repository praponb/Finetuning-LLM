SELECT *, SUBSTRING(status, 1, 3) AS status_prefix, TRIM(status) AS clean_status
FROM shipments
WHERE order_id = 24
