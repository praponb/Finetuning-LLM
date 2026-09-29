SELECT id, duration_sec, page
FROM web_events
WHERE duration_sec BETWEEN 177 AND 196
ORDER BY duration_sec DESC
