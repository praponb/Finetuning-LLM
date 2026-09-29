WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded
    FROM payments
    WHERE paid_at >= DATE '2023-06-22'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT method, status
    FROM valid
    EXCEPT ALL
    SELECT method, status
    FROM payments
    WHERE method = 'cash'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.method = k.method AND s.status = k.status
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, o.order_date
    FROM kept_rows a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM with_dims
    EXCEPT ALL
    SELECT order_id, status
    FROM payments
    WHERE order_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.status = k.status
)
-- Grouped metrics
SELECT
    status,
    COUNT(DISTINCT method) AS distinct_methods,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT_IF(method = 'promptpay') AS promptpay_events
FROM verified
GROUP BY status
ORDER BY status DESC NULLS LAST;
