WITH session_totals AS (
    SELECT session_id, COUNT(*) AS total_count, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id
)
SELECT s.device, st.total_count, st.duration_sec_range
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
WHERE st.duration_sec_range >= 27
ORDER BY 3 DESC, s.device;
