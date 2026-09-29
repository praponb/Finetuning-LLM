WITH flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_tier,
    COUNT(*) FILTER (WHERE event_type = 'click') AS click_rows,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    COUNT(DISTINCT session_id) AS distinct_session_ids
FROM sessionized
GROUP BY CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT session_id) >= 1
ORDER BY checkout_rows NULLS FIRST
