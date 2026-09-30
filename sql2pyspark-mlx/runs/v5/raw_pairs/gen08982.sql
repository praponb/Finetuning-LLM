SELECT
    page,
    ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search,
    COUNT(*) AS num_web_events,
    MIN(event_time) AS first_event_time
FROM web_events
WHERE session_id IN (2, 7, 22, 36)
GROUP BY page
ORDER BY num_web_events DESC, page
