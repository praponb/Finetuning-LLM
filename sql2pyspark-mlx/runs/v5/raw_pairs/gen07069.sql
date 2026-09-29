WITH gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS session_start_flag_tier,
    SUM(session_id) FILTER (WHERE device = 'desktop') AS desktop_total_session_id,
    PERCENTILE_APPROX(session_id, 0.9) AS session_id_p90,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    COUNT(*) AS event_count
FROM islands
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING PERCENTILE_APPROX(session_id, 0.9) >= 0
ORDER BY event_count NULLS LAST;
