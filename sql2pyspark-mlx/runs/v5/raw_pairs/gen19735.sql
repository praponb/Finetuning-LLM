WITH gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
)
SELECT
    id,
    device,
    browser,
    started_at,
    SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM gaps
