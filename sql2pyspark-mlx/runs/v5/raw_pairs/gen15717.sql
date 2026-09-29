SELECT id, duration_sec, COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE duration_sec <= 178 AND event_type NOT IN ('page_view', 'click');
