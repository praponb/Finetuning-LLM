WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE TO_DATE(event_time) >= '2023-07-28'
),
agg AS (
    SELECT event_type, MIN(duration_sec) AS min_duration_sec
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, min_duration_sec
FROM agg
ORDER BY min_duration_sec DESC
