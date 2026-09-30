WITH session_totals AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    WHERE session_id = 27
    GROUP BY session_id
)
SELECT s.country, s.browser, st.total_duration_sec
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id;
