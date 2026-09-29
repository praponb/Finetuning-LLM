WITH top_shipments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY delivered_date) AS row_num
    FROM shipments
)
SELECT *
FROM top_shipments
WHERE row_num = 1
ORDER BY order_id, status
