WITH session_stats AS (
    SELECT session_id, ROUND((100 * SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.started_at, ss.pct_page_view
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.pct_page_view >= 54;
