select s.* from sessions s where exists (select 1 from web_events c where c.session_id = s.id and c.event_time <= s.started_at);
