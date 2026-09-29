SELECT customer_id, country, browser FROM sessions WHERE device = 'desktop' AND (device = 'desktop' OR customer_id IN (4, 22))
