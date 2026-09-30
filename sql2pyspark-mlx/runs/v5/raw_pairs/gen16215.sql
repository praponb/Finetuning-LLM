SELECT we.id, we.session_id, we.page FROM web_events we WHERE we.duration_sec <= 291 AND we.session_id NOT IN (SELECT id FROM sessions WHERE device <> 'desktop');
