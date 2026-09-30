SELECT id, session_id, event_time, PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY event_time) AS event_time_pct_rank FROM web_events
