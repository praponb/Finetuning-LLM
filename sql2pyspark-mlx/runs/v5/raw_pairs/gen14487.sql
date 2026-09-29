select s.id, s.device, count(we.id) as web_event_count from sessions s inner join web_events we on we.session_id = s.id where we.event_type = 'add_to_cart' group by s.id, s.device;
