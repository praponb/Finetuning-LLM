select e.*, s.browser, s.device from web_events e left join sessions s on e.session_id = s.id
