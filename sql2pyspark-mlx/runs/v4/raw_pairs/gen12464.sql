SELECT page, MIN(duration_sec) AS min_duration_sec FROM web_events WHERE event_type IN ('search', 'purchase', 'page_view') GROUP BY 1;
