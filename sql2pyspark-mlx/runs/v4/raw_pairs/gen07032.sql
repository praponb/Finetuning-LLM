SELECT YEAR(event_time) event_year, MAX(duration_sec) max_duration_sec
FROM web_events
WHERE page = '/search'
GROUP BY 1
ORDER BY event_year DESC, max_duration_sec DESC
