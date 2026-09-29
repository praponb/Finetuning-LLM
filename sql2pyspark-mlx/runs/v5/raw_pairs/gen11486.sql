WITH session_totals AS (
    SELECT session_id, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    st.latest_event_time
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
ORDER BY customer_id DESC, id
LIMIT 5
