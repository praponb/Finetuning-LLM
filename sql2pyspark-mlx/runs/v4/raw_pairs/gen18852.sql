SELECT browser, MIN(started_at) AS first_started_at, COUNT(DISTINCT country) AS distinct_countries FROM sessions GROUP BY browser HAVING distinct_countries > 1 ORDER BY browser DESC;
