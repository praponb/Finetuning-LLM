SELECT event_type, avg_duration_sec
FROM (
    SELECT event_type, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY event_type
) AS s
WHERE avg_duration_sec > 250
ORDER BY event_type
LIMIT 10
