SELECT id, event_time, RANK() OVER (ORDER BY event_time) AS event_time_rank
FROM web_events
WHERE event_type = 'page_view';
