SELECT id, customer_id, browser, TRIM(device) AS clean_device FROM sessions WHERE started_at < '2023-09-01' OR device = 'mobile' ORDER BY customer_id
