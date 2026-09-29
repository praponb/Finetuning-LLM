WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    page,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    MIN(event_time) AS first_event_time,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
    APPROX_PERCENTILE(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles
FROM sessions_web_events
GROUP BY page
ORDER BY page NULLS LAST;
