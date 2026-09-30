SELECT we.id, we.duration_sec, we.session_id, we.event_type, s.country FROM web_events we RIGHT JOIN sessions s ON we.session_id = s.id ORDER BY event_type;
