SELECT id, started_at FROM sessions WHERE customer_id IS NOT NULL AND started_at >= '2024-06-01' ORDER BY id ASC LIMIT 10
