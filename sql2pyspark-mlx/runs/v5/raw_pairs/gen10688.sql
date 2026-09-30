WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    SIZE(COLLECT_SET(method)) AS num_methods,
    MIN(paid_at) AS first_seen,
    COUNT(*) AS event_count
FROM sessions_payments
GROUP BY CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING event_count >= 1
ORDER BY session_id_tier DESC NULLS LAST
