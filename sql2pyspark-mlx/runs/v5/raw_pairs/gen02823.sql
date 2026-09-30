WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM with_gaps
