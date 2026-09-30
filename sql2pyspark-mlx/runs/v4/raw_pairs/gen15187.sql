SELECT id, started_at, browser, customer_id, HOUR(started_at) AS started_at_hour FROM sessions WHERE device = 'desktop' ORDER BY 3, started_at_hour DESC;
