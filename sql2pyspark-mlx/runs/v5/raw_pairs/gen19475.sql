WITH top_shipments AS (
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS row_num
    FROM shipments
)
SELECT *
FROM top_shipments
WHERE row_num <= 3
ORDER BY id, row_num
