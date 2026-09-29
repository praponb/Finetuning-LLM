SELECT browser, country FROM sessions WHERE started_at >= '2025-04-25' UNION SELECT browser, country FROM sessions WHERE customer_id IN (1, 19)
