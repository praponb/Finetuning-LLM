SELECT page FROM web_events EXCEPT DISTINCT SELECT page FROM web_events WHERE duration_sec IS NOT NULL
