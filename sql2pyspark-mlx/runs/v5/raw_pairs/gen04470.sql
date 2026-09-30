WITH with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country,
    SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM with_gaps;
