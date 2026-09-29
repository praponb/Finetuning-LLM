WITH remaining_keys AS (
    -- Set difference on keys
    SELECT method
    FROM payments
    EXCEPT DISTINCT
    SELECT method
    FROM payments
    WHERE order_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    JOIN remaining_keys k ON s.method = k.method
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    status,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'GROUP_A' WHEN status RLIKE '^(FAILED)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone
FROM kept_rows
ORDER BY order_id
