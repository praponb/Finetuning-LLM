WITH recent_web_events AS (
    SELECT *, CEIL(duration_sec) AS duration_sec_whole
    FROM web_events
    WHERE page IS NOT NULL AND event_type = 'page_view' AND event_time < '2025-09-01'
),
web_events_stats AS (
    SELECT event_type, COUNT(duration_sec) AS duration_sec_count
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, duration_sec_count
FROM web_events_stats
WHERE duration_sec_count > 7
ORDER BY duration_sec_count DESC;
