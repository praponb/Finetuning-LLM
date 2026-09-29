SELECT event_type, id
FROM web_events
EXCEPT DISTINCT
SELECT event_type, id
FROM web_events
WHERE event_time < '2023-09-18'
