SELECT id, country, started_at, LEAD(started_at, 2) OVER (PARTITION BY country ORDER BY started_at, id) AS next_started_at FROM sessions WHERE device IN ('mobile', 'tablet', 'desktop')
