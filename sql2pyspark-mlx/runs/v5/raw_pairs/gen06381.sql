select we.session_id, we.event_type, we.duration_sec, s.started_at, s.customer_id from web_events we inner join sessions s on we.session_id = s.id where s.customer_id is not null;
