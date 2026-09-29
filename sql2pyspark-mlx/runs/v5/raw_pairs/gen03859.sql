SELECT
    id,
    shipped_date,
    delivered_date,
    carrier
FROM shipments
WHERE delivered_date BETWEEN '2023-08-01' AND '2023-09-07';
