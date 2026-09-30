SELECT session_id, page, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events
WHERE event_type IN ('search', 'click', 'page_view') AND (duration_sec > 214 OR page <> '/cart')
GROUP BY session_id, page
ORDER BY 3 DESC;
