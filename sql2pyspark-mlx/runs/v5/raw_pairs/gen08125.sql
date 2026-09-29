SELECT
    id,
    carrier,
    cost,
    delivered_date
FROM (
    SELECT
        id,
        carrier,
        cost,
        delivered_date,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS row_num
    FROM shipments
) AS ranked
WHERE row_num <= 3
