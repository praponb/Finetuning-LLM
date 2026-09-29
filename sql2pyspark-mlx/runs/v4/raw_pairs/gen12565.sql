WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CONCAT_WS('-', status, method) AS status_method_key,
        LENGTH(status) AS status_length
    FROM payments
    WHERE amount BETWEEN 670 AND 1030
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    burst_id,
    CASE WHEN status_length >= 64 THEN 'HIGH' ELSE 'LOW' END AS status_length_tier,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    COUNT(*) FILTER (WHERE method = 'promptpay') AS promptpay_rows
FROM sessions_payments
GROUP BY burst_id, CASE WHEN status_length >= 64 THEN 'HIGH' ELSE 'LOW' END
HAVING promptpay_rows >= 1
ORDER BY promptpay_rows DESC NULLS LAST;
