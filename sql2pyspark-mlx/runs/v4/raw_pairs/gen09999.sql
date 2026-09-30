select s.*, x.duration_sec from sessions s join lateral (select c.duration_sec from web_events c where c.session_id = s.id order by c.duration_sec desc, c.id desc limit 1) x on true
