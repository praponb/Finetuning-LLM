SELECT *, CASE WHEN page IN ('/cart', '/home') THEN 'Group A' ELSE 'Other' END AS page_group, CASE WHEN event_type = 'click' THEN 1 ELSE 0 END AS is_click
FROM web_events
WHERE session_id = 27;
