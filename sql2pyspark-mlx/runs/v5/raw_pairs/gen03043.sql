WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS pos
    FROM web_events
)
SELECT *
FROM numbered
WHERE pos <= 2
ORDER BY duration_sec DESC, session_id DESC;
