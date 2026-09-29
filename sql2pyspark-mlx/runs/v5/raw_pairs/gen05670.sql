SELECT s.device, MIN(we.event_time) first_event_time, MAX(we.event_time) last_event_time
FROM web_events we
JOIN sessions s ON s.id = we.session_id
LEFT JOIN customers c ON c.id = s.customer_id
GROUP BY s.device;
