select w.id, w.event_type, w.duration_sec from web_events w where w.duration_sec = (select max(w2.duration_sec) from web_events w2 where w2.event_type = w.event_type) order by w.event_type desc;
