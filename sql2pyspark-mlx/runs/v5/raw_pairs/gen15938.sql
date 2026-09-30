SELECT *, DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label, TO_DATE(event_time) AS event_time_date FROM web_events WHERE session_id IN (14, 31)
