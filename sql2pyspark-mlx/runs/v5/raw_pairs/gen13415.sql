SELECT page
FROM web_events
WHERE event_type <> 'search'
UNION
SELECT page
FROM web_events
WHERE session_id IN (1, 19, 28)
ORDER BY page DESC;
