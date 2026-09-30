SELECT id, event_type, CAST(duration_sec AS STRING) AS duration_sec_num
FROM web_events
ORDER BY id DESC;
