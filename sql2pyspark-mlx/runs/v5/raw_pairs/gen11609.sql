SELECT s.country, s.started_at, we.page, we.event_time FROM sessions s LEFT OUTER JOIN web_events we ON we.session_id = s.id;
