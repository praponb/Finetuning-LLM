SELECT page, web_event_count, search_count
FROM (
    SELECT page, COUNT(*) AS web_event_count, COUNT(CASE WHEN page = '/search' THEN 1 END) AS search_count
    FROM web_events
    GROUP BY page
) AS t
WHERE search_count > 4;
