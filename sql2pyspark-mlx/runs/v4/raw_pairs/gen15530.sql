WITH SessionSummary AS (
    SELECT session_id, COUNT(CASE WHEN page = '/home' THEN 1 END) home_count, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) page_view_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.browser,
    s.country,
    S.home_count,
    S.page_view_count
FROM sessions s
INNER JOIN SessionSummary S ON s.id = S.session_id
WHERE S.page_view_count >= 50
ORDER BY S.page_view_count, s.browser;
