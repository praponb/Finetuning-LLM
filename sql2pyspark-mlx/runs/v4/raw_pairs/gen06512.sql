SELECT id, customer_id, LOWER(device) AS device_lower FROM sessions WHERE customer_id IN (21, 34) OR customer_id IN (1, 15, 23) ORDER BY device_lower, customer_id DESC, id LIMIT 5
