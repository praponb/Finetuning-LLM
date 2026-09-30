SELECT session_id, event_time, DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label, DATE_TRUNC('WEEK', event_time) AS event_time_period FROM web_events WHERE TO_DATE(event_time) < '2025-09-01'
