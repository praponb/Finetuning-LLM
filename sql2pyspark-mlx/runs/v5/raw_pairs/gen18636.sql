SELECT
    shipped_date,
    carrier,
    status,
    TRIM(status) AS clean_status
FROM shipments
WHERE order_id IN (17, 35)
ORDER BY clean_status
