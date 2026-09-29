WITH numbered AS (
    SELECT
        id,
        status,
        shipped_date,
        carrier,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) row_num
    FROM shipments
)
SELECT
    id,
    status,
    shipped_date,
    carrier
FROM numbered
WHERE row_num <= 2
ORDER BY status DESC, carrier;
