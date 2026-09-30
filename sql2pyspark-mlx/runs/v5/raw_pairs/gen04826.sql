WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS row_num
    FROM shipments
)
SELECT *
FROM top_shipments
WHERE row_num <= 3
ORDER BY id DESC
