SELECT DATE_FORMAT(event_time, 'yyyy-MM') AS event_month, event_type, page, MIN(event_time) AS first_event_time FROM web_events GROUP BY 1, 2, 3;
