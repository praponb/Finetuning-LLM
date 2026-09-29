select a.id, a.event_type, a.duration_sec, a.session_id from web_events a where a.duration_sec = (select max(b.duration_sec) from web_events b where b.event_type = a.event_type);
