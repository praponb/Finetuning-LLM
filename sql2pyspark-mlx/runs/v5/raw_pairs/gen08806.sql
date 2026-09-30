SELECT started_at, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at FROM sessions WHERE customer_id = 33
