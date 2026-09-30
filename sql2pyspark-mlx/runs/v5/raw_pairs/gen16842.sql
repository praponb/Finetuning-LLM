WITH ranked AS (SELECT id, country FROM sessions QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) = 1) SELECT id FROM ranked ORDER BY id DESC
