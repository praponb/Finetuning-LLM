SELECT we.id, we.event_type, we.page, we.event_time, s.browser FROM web_events we RIGHT JOIN sessions s ON we.session_id = s.id WHERE we.page = '/search' AND we.page IN ('/cart', '/search', '/home');
