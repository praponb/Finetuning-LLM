SELECT COUNT(DISTINCT duration_sec) AS unique_sessions, MAX(duration_sec) AS max_duration_sec FROM web_events WHERE event_type NOT IN ('click', 'page_view', 'purchase') ORDER BY unique_sessions
