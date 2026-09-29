WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS is_new_burst_per_failed,
    SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    MIN(paid_at) AS started_at,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    MAX(paid_at) AS ended_at
FROM sessionized
GROUP BY ROLLUP (CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END)
HAVING refunded_events < 1
ORDER BY refunded_events DESC;
