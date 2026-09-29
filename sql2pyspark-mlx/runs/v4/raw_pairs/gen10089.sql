SELECT we.id, we.session_id, we.page, we.event_type FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE started_at < '2025-01-27')
