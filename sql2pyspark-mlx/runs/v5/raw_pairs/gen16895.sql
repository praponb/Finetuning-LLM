WITH gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    is_new_burst,
    session_no,
    CASE WHEN event_type RLIKE '^(page_view|purchase)$' THEN 'GROUP_B' WHEN event_type RLIKE '^(click|add_to_cart)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS event_type_zone
FROM sessions_web_events
ORDER BY event_type_zone;
