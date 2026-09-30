WITH remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM payments
    EXCEPT DISTINCT
    SELECT order_id, status
    FROM payments
    WHERE order_id IN (8, 11, 15, 30)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.status = k.status
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50
FROM verified
GROUP BY DATE_TRUNC('YEAR', paid_at)
HAVING PERCENTILE_APPROX(amount, 0.5) > 0
ORDER BY duration_min DESC NULLS FIRST;
