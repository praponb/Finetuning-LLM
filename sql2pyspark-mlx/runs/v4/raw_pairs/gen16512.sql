WITH top_shipments AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS rn
    FROM shipments
)
SELECT id, order_id, shipped_date
FROM top_shipments
WHERE rn <= 3
