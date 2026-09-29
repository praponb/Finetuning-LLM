SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY browser, device ORDER BY started_at DESC) AS dedup_rank FROM sessions) t WHERE dedup_rank = 1;
