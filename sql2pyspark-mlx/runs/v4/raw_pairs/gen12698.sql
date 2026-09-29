SELECT we.id, we.event_type, we.duration_sec, we.session_id FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE device <> 'desktop') ORDER BY 2
