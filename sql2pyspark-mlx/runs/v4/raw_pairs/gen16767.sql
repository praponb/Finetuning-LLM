SELECT id, started_at, COALESCE(browser, 'Unknown') AS browser_filled FROM sessions WHERE customer_id IS NULL AND country IN ('SG', 'DE', 'US') AND customer_id IN (17, 40)
