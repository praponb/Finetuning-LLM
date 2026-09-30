WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE session_id = 37 AND event_type <> 'add_to_cart'
),
web_events_stats AS (
    SELECT DATE_FORMAT(event_time, 'yyyy-MM') AS event_month, MIN(duration_sec) AS min_duration_sec
    FROM filtered_web_events
    GROUP BY DATE_FORMAT(event_time, 'yyyy-MM')
)
SELECT *
FROM web_events_stats
ORDER BY min_duration_sec DESC, event_month;
