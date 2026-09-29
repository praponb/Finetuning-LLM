WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 496 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    ROUND(AVG(session_no), 2) AS avg_session_no,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
    SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list
FROM islands
GROUP BY CASE WHEN duration_sec >= 496 THEN 'HIGH' ELSE 'LOW' END
