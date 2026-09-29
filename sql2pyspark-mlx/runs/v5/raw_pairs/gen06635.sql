SELECT id, duration_sec, session_id, page, SUBSTRING(page, 1, 2) AS page_prefix FROM web_events WHERE session_id IN (6, 13, 17, 30)
