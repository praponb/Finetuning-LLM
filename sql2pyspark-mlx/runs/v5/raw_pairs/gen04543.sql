SELECT we.event_time, we.page, we.event_type, s.browser FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id ORDER BY event_type;
