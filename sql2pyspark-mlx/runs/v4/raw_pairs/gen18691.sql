WITH FilteredWebEvents AS (
    SELECT *
    FROM web_events
    WHERE NOT (session_id = 32 AND session_id = 38 AND session_id = 35)
),
event_type_summary AS (
    SELECT event_type, MIN(event_time) AS first_event_time
    FROM FilteredWebEvents
    GROUP BY event_type
)
SELECT *
FROM event_type_summary
