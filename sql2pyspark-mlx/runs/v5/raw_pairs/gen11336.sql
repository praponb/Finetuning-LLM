SELECT *, RANK() OVER (PARTITION BY country, browser ORDER BY started_at DESC) AS latest_rank FROM sessions QUALIFY latest_rank = 1
