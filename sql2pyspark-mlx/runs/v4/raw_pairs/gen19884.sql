WITH kept_keys AS (
    SELECT order_id, status
    FROM payments
    INTERSECT ALL
    SELECT order_id, status
    FROM payments
    WHERE order_id IS NULL
),
reconciled AS (
    SELECT s.*
    FROM payments s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
)
SELECT
    id,
    order_id,
    amount,
    status,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'GROUP_A' ELSE 'CORE' END AS status_zone
FROM reconciled
ORDER BY status DESC;
