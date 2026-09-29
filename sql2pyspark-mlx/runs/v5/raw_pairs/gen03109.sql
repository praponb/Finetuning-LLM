SELECT p.browser, s.event_type, COUNT(*) AS rows_total, ROUND(AVG(s.duration_sec), 2) AS avg_duration_sec FROM web_events s JOIN sessions p ON s.session_id = p.id GROUP BY p.browser, s.event_type;
