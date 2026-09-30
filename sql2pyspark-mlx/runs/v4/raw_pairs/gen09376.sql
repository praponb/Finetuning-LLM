SELECT device, country FROM sessions WHERE customer_id = 1 UNION ALL SELECT device, country FROM sessions WHERE started_at < '2023-10-26'
