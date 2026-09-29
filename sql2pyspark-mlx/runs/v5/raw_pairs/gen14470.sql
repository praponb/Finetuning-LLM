select s.customer_id, we.page, we.session_id, we.event_time from sessions s join web_events we on s.id = we.session_id
