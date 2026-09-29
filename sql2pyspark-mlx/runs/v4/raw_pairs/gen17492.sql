WITH kept_keys AS (
    SELECT order_id, id
    FROM payments
    INTERSECT
    SELECT order_id, id
    FROM payments
    WHERE status = 'FAILED'
),
kept_rows AS (
    SELECT s.*
    FROM payments s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    id,
    order_id,
    amount
FROM kept_rows
ORDER BY id DESC;
