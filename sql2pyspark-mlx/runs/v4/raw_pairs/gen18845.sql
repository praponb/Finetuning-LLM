select s.country, s.customer_id, s.device, we.duration_sec, we.page, we.session_id from sessions s left join web_events we on s.id = we.session_id order by s.country;
