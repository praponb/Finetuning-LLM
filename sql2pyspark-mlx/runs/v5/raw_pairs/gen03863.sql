select s.id, s.country from sessions s where s.browser in ('Firefox', 'Safari') and exists (select 1 from web_events we where we.session_id = s.id);
