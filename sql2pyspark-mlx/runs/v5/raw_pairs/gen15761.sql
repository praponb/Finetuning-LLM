SELECT device, customer_id FROM sessions WHERE customer_id IS NOT NULL AND device NOT IN ('mobile', 'desktop') AND browser NOT IN ('Edge', 'Chrome', 'Safari')
