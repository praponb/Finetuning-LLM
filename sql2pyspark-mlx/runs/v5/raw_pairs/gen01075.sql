select s.* from sessions s where exists (select 1 from web_events ch where ch.session_id = s.id and ch.event_time <= s.started_at);
