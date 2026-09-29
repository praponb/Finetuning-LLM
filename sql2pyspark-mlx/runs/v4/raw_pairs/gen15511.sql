select e.*, p.browser, p.device from web_events e join sessions p on e.session_id = p.id
