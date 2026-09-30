WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    method,
    amount,
    paid_at,
    session_start_flag
FROM sessionized
ORDER BY paid_at DESC;
