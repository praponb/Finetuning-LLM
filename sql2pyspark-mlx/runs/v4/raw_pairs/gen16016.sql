WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY discount DESC) AS rn
    FROM orders
    WHERE status = 'SHIPPED'
)
SELECT
    id,
    status,
    discount,
    amount
FROM ranked
WHERE rn <= 5
ORDER BY status DESC
