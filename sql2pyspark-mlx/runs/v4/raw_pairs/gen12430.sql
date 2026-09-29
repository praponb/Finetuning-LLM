WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    order_id,
    status,
    session_start_flag,
    island_id
FROM sessions_payments
ORDER BY order_id DESC NULLS LAST
