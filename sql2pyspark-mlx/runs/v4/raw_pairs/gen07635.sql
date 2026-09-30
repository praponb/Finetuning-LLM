SELECT id, started_at, country FROM sessions WHERE TO_DATE(started_at) < '2025-08-01' ORDER BY country DESC;
