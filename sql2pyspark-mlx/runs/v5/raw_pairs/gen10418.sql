WITH session_totals AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.device,
    s.started_at,
    st.total_duration_sec,
    st.avg_duration_sec
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY avg_duration_sec, total_duration_sec
