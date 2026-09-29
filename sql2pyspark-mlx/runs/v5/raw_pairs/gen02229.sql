WITH FilteredWebEvents AS (
    SELECT *
    FROM web_events
    WHERE duration_sec = 445
),
EventTypeGroupSummary AS (
    SELECT CASE WHEN event_type IN ('click', 'purchase') THEN 'core' ELSE 'other' END event_type_group, ROUND(AVG(duration_sec), 2) avg_duration_sec
    FROM FilteredWebEvents
    GROUP BY CASE WHEN event_type IN ('click', 'purchase') THEN 'core' ELSE 'other' END
)
SELECT event_type_group, avg_duration_sec
FROM EventTypeGroupSummary
ORDER BY event_type_group
LIMIT 10;
