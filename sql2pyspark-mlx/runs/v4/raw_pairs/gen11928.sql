SELECT event_type, COUNT(*) AS total_count FROM web_events GROUP BY event_type ORDER BY total_count DESC, event_type LIMIT 20;
