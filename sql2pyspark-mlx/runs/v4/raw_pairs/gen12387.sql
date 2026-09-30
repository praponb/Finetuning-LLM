SELECT s.customer_id, s.browser, we.event_time, we.session_id, we.event_type FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id WHERE s.country IN ('JP', 'US')
