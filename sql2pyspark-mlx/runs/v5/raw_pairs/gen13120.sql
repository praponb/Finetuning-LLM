select s.device, s.customer_id, we.event_time from sessions s join web_events we on s.id = we.session_id order by we.event_time desc, s.device;
