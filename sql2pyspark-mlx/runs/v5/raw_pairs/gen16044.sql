SELECT id, browser, country, device FROM sessions WHERE customer_id IS NULL OR browser = 'Edge' ORDER BY country, browser
