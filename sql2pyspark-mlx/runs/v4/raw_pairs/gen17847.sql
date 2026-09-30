select s.id, s.customer_id, s.browser from sessions s where s.customer_id is not null and not exists (select 1 from web_events we where we.session_id = s.id and we.duration_sec > 240);
