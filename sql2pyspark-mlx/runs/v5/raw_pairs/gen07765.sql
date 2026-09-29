WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) row_num
    FROM shipments
)
SELECT
    id,
    status,
    cost,
    order_id
FROM ranked
WHERE row_num <= 1
ORDER BY cost
