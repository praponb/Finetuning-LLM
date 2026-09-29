SELECT page, session_id
FROM web_events
INTERSECT
SELECT page, session_id
FROM web_events
WHERE page = '/product';
