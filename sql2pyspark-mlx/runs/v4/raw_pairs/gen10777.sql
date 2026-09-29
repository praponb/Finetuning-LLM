SELECT *, DATE_TRUNC('year', event_time) AS event_time_period
FROM web_events
WHERE page <> '/checkout'
ORDER BY duration_sec, id
LIMIT 10;
