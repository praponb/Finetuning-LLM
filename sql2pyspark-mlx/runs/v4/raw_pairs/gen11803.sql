SELECT *
FROM web_events
WHERE duration_sec BETWEEN 243 AND 361 AND TO_DATE(event_time) >= '2024-07-01'
