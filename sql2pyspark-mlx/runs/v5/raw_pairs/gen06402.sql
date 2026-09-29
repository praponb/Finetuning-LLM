SELECT s.started_at, s.country, we.event_type, we.duration_sec, we.event_time FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.country IN ('US', 'SG');
