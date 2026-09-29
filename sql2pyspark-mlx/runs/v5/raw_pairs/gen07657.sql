SELECT
    id,
    carrier,
    delivered_date,
    status
FROM (
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS row_num
    FROM shipments
    WHERE cost BETWEEN 72 AND 75
) AS t
WHERE row_num = 1
