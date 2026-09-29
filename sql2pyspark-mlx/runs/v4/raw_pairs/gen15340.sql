SELECT * FROM web_events QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) = 1;
