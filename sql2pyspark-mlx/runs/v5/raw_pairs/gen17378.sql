SELECT id, page, event_type
FROM web_events
WHERE duration_sec >= 352
ORDER BY page DESC, event_type DESC;
