WITH numbered AS (
    SELECT
        id,
        order_id,
        amount,
        method,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) pos
    FROM payments
)
SELECT
    id,
    order_id,
    amount,
    method
FROM numbered
WHERE pos = 1
ORDER BY id, 4
