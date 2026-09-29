SELECT s.started_at, s.browser, we.session_id, we.event_type FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.country NOT IN ('US', 'TH', 'JP') ORDER BY browser DESC;
