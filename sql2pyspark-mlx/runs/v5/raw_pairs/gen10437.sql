SELECT *, TRIM(page) AS clean_page, COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE event_time < '2023-07-01' AND (duration_sec IS NOT NULL OR duration_sec < 398);
