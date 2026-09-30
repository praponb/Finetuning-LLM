SELECT *, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time, QUARTER(event_time) AS event_time_quarter
FROM web_events
WHERE duration_sec BETWEEN 166 AND 283
ORDER BY event_time;
