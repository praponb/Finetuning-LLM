SELECT s.id, s.device, s.customer_id
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment = 'SMB')
