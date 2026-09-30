select s.browser, s.started_at, we.duration_sec from sessions s left join web_events we on we.session_id = s.id where we.duration_sec <= 342;
