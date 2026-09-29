SELECT id, duration_sec, page, CASE WHEN page = '/checkout' THEN 1 ELSE 0 END AS is_checkout FROM web_events WHERE page IS NOT NULL ORDER BY id;
