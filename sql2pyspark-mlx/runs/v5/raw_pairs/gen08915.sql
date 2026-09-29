SELECT duration_sec, event_type, DATE_TRUNC('year', event_time) AS event_time_period FROM web_events WHERE page = '/home';
