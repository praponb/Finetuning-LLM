WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY amount, id) = 1
)
SELECT
    id,
    order_id,
    amount
FROM ranked
