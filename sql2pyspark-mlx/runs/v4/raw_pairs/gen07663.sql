SELECT DISTINCT method, status
FROM payments
ORDER BY status, method
LIMIT 1
