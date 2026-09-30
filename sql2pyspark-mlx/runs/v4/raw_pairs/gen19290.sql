SELECT we.session_id, we.event_time, s.browser, s.country FROM web_events we INNER JOIN sessions s ON s.id = we.session_id
