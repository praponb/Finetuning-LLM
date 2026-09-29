SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) AS dedup_rank FROM web_events) t WHERE dedup_rank = 1
