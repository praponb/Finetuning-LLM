select we.duration_sec, s.browser, s.started_at, c.age from web_events we join sessions s on s.id = we.session_id left join customers c on s.customer_id = c.id where c.is_active = true
