SELECT * FROM (SELECT id, browser, started_at, device, RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) AS pos FROM sessions) AS ranked WHERE pos = 1
