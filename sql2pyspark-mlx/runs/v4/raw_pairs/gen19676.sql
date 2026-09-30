SELECT page, MAX(duration_sec) AS max_duration_sec, COUNT(DISTINCT duration_sec) AS unique_pages
FROM web_events
GROUP BY page
HAVING unique_pages < 2
ORDER BY unique_pages;
