SELECT c.is_active, COUNT(DISTINCT s.device) AS unique_devices
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
