WITH session_totals AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec, COUNT(DISTINCT duration_sec) AS unique_sessions
    FROM web_events
    WHERE page IN ('/home', '/search')
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    st.max_duration_sec,
    st.unique_sessions
FROM sessions s
LEFT OUTER JOIN session_totals st ON s.id = st.session_id
