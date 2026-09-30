WITH src AS (
    SELECT *
    FROM web_events
    WHERE page IN ('/product', '/home', '/search') AND duration_sec > 432
),
summary AS (
    SELECT session_id, COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM src
    GROUP BY session_id
)
SELECT session_id, avg_duration_sec
FROM summary
WHERE avg_duration_sec < 75
ORDER BY session_id;
