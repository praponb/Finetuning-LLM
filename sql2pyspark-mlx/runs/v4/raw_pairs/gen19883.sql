SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at NULLS FIRST, id) AS latest_rank
FROM sessions
QUALIFY latest_rank = 1
