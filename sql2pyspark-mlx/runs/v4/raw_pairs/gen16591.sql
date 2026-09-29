SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id ASC) AS latest_rank
FROM sessions
QUALIFY latest_rank = 1;
