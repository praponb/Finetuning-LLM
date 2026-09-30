WITH top_shipments AS (
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS rnk
    FROM shipments
    WHERE cost IS NOT NULL
)
SELECT
    id,
    carrier,
    shipped_date,
    cost
FROM top_shipments
WHERE rnk = 1;
