SELECT *, YEAR(event_time) AS event_time_year, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time FROM web_events WHERE duration_sec > 301
