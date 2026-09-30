SELECT
    cost,
    delivered_date,
    shipped_date,
    SUBSTRING(status, 1, 2) AS status_prefix
FROM shipments
WHERE shipped_date >= '2024-02-09'
