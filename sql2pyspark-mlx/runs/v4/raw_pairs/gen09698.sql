WITH with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    MIN(session_id_2) AS min_session_id_2,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    AVG(duration_sec) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_avg_duration_sec,
    SUM(duration_sec) FILTER (WHERE page = '/search') AS search_total_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    MAX(event_time) AS last_event_time
FROM sessions_web_events
GROUP BY DATE_TRUNC('WEEK', event_time), CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END
HAVING SUM(duration_sec) FILTER (WHERE page = '/search') >= 0
ORDER BY add_to_cart_avg_duration_sec NULLS FIRST;
