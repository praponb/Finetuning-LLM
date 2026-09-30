select a.id, a.event_type, a.duration_sec from web_events a where a.duration_sec = (select min(b.duration_sec) from web_events b where b.event_type = a.event_type)
