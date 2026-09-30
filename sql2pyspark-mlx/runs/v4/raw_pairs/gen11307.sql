SELECT id, event_type, page
FROM web_events
WHERE page = '/cart'
INTERSECT
SELECT id, event_type, page
FROM web_events
WHERE page IN ('/search', '/product', '/cart')
