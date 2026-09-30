SELECT id, event_time, RANK() OVER (ORDER BY event_time) AS rnk FROM web_events
