select customer_id, country, hour(started_at) as started_at_hour from sessions where (customer_id = 40 and device = 'mobile') or started_at < '2024-12-05'
