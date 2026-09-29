select s.id, s.browser, s.customer_id from sessions s where exists (select 1 from web_events we where we.session_id = s.id);
