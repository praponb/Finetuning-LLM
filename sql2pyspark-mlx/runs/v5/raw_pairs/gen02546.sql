SELECT id, customer_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at NULLS LAST, id) AS latest_rank FROM sessions) ranked WHERE latest_rank = 1
