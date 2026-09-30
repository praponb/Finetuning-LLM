SELECT page, min_event_time, unique_pages
FROM (
    SELECT page, MIN(event_time) AS min_event_time, COUNT(DISTINCT duration_sec) AS unique_pages
    FROM web_events
    GROUP BY page
) AS s
WHERE unique_pages < 6
