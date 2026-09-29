select s.id, s.browser, s.started_at from sessions s where s.device = 'desktop' and not exists (select 1 from web_events we where we.session_id = s.id and we.duration_sec <= 304);
