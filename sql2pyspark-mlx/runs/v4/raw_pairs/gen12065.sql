select we.id, we.session_id from web_events we where we.session_id in (select id from sessions where customer_id in (12, 18, 36));
