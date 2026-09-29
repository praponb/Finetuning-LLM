WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    CASE WHEN amount >= 1640 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    AVG(session_id) AS avg_session_id,
    MAX(paid_at) AS last_paid_at,
    SUM(session_id) FILTER (WHERE status = 'FAILED') AS failed_total_session_id
FROM sessions_payments
GROUP BY DATE_TRUNC('MONTH', paid_at), CASE WHEN amount >= 1640 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY failed_total_session_id NULLS LAST;
