SELECT status
FROM accounts
EXCEPT
SELECT status
FROM shipments
WHERE order_id = 38
ORDER BY status DESC;
