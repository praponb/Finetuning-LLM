WITH numbered AS (
    SELECT
        id,
        status,
        discount,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS pos
    FROM orders
    WHERE status IN ('PENDING', 'RETURNED')
)
SELECT
    id,
    status,
    discount,
    order_date
FROM numbered
WHERE pos <= 3
ORDER BY status DESC
