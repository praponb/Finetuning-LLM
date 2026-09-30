SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS LAST, id) AS latest_rank
FROM web_events
QUALIFY latest_rank = 1;
