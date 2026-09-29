WITH flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    CASE WHEN amount >= 1640 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    COUNT(DISTINCT method) AS distinct_methods,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM sessions_payments
GROUP BY CASE WHEN amount >= 1640 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY failed_count NULLS FIRST
