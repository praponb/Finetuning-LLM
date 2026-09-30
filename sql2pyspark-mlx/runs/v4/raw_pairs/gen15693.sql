select s.* from sessions s where not exists (select 1 from web_events x where x.session_id = s.id and x.event_time <= s.started_at);
