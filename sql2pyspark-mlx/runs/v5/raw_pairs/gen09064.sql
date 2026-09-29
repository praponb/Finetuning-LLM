select s.id, s.started_at from sessions s where s.id in (select session_id from web_events where event_time >= '2024-05-01')
