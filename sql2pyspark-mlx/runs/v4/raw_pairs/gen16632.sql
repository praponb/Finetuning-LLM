SELECT status
FROM payments
EXCEPT
SELECT status
FROM shipments
ORDER BY status
