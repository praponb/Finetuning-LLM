SELECT id, page, event_type, duration_sec, CAST(duration_sec AS STRING) duration_sec_str, WEEKOFYEAR(event_time) event_time_weekofyear FROM web_events WHERE TO_DATE(event_time) < '2025-06-05';
