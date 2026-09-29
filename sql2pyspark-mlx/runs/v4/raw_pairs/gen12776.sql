select we.duration_sec, we.page, s.customer_id from web_events we inner join sessions s on we.session_id = s.id;
