SELECT id, country, started_at, browser FROM (SELECT id, country, started_at, browser, DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at DESC) rnk FROM sessions) ranked WHERE rnk <= 1
