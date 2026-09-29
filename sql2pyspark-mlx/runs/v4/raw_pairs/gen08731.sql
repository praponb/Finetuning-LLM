WITH SessionSummary AS (
    SELECT session_id, AVG(duration_sec) AS avg_duration_sec, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec
    FROM web_events
    WHERE page = '/cart'
    GROUP BY session_id
)
SELECT
    sessions.browser,
    sessions.customer_id,
    SessionSummary.avg_duration_sec,
    SessionSummary.add_to_cart_duration_sec
FROM sessions
LEFT JOIN SessionSummary ON sessions.id = SessionSummary.session_id
