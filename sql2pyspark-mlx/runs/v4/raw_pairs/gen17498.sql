SELECT web_events.event_time, sessions.country, CASE WHEN sessions.device = 'tablet' THEN 1 ELSE 0 END AS is_tablet
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
ORDER BY event_time DESC;
