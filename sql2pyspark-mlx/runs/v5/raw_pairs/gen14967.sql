SELECT we.event_time, s.country, CASE WHEN we.event_type = 'add_to_cart' THEN 1 ELSE 0 END AS is_add_to_cart
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.country = 'UK' AND we.page NOT IN ('/home', '/product')
ORDER BY event_time ASC;
