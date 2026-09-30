SELECT * FROM sessions QUALIFY RANK() OVER (PARTITION BY browser, customer_id ORDER BY started_at DESC NULLS LAST) = 1;
