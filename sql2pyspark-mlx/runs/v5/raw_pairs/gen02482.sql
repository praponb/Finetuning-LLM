SELECT
    id,
    delivered_date,
    status,
    INITCAP(TRIM(carrier)) AS carrier_title
FROM shipments
WHERE shipped_date < '2024-05-01';
