select s.country, s.customer_id, s.browser, we.event_type from sessions s left outer join web_events we on we.session_id = s.id where s.device = 'mobile' and we.duration_sec <> 146;
