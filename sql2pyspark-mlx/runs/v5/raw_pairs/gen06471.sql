SELECT SUM(CASE WHEN page = '/checkout' THEN duration_sec ELSE 0 END) AS checkout_duration_sec FROM web_events;
