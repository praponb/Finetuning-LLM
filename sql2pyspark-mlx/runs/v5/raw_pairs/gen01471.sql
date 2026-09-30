WITH with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country,
    SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM with_gaps
