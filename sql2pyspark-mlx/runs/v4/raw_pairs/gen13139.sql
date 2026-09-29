WITH base AS (
    SELECT *, CONCAT_WS(',', page, event_type) AS page_event_type_key
    FROM web_events
    WHERE duration_sec >= 284
),
event_type_summary AS (
    SELECT event_type, MAX(event_time) AS latest_event_time, SUM(duration_sec) AS total_duration_sec
    FROM base
    GROUP BY event_type
)
SELECT *
FROM event_type_summary
ORDER BY event_type
LIMIT 5
