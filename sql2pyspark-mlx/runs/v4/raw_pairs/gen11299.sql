WITH gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    status,
    MIN(paid_at) AS first_seen,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    MIN(paid_at) AS first_seen_2,
    SIZE(COLLECT_SET(status)) AS num_status,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles
FROM sessions_payments
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), status), (DATE_TRUNC('YEAR', paid_at)))
