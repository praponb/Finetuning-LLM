SELECT we.id, we.event_type FROM web_events we WHERE we.event_time < '2025-02-01' AND we.session_id NOT IN (SELECT id FROM sessions WHERE started_at >= '2025-06-19')
