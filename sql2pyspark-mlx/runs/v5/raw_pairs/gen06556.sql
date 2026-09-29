SELECT id, browser, device, started_at, LEAD(started_at, 1) OVER (PARTITION BY browser, device ORDER BY started_at, id) AS next_started_at FROM sessions;
