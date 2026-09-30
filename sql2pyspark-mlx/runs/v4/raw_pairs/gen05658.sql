SELECT id, started_at, LEAD(started_at, 1) OVER (ORDER BY started_at, id) next_started_at
FROM sessions
ORDER BY id ASC;
