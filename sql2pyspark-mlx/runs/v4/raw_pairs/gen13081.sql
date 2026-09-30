SELECT status
FROM accounts
EXCEPT
SELECT status
FROM shipments;
