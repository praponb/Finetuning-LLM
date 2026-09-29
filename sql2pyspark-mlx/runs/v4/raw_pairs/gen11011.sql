WITH session_summary AS (
    SELECT session_id, COUNT(*) num_web_events
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    s.country,
    ss.num_web_events
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.num_web_events >= 8
ORDER BY s.id DESC;
