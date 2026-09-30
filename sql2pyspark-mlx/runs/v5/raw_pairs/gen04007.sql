SELECT s.country, s.browser, we.event_type, we.session_id FROM sessions s INNER JOIN web_events we ON we.session_id = s.id WHERE s.country IS NOT NULL;
