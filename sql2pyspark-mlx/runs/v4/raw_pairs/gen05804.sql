SELECT event_time, HOUR(event_time) AS event_time_hour FROM web_events WHERE duration_sec BETWEEN 126 AND 320;
