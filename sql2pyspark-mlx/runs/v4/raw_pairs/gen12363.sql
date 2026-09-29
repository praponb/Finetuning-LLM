WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    session_start_flag,
    island_id,
    CASE WHEN event_type RLIKE '^(click|search)$' THEN 'CORE' WHEN event_type RLIKE '^(page_view|add_to_cart)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS event_type_zone
FROM islands
