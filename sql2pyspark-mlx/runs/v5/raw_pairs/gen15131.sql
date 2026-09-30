SELECT amount
FROM transactions
EXCEPT
SELECT cost
FROM shipments
ORDER BY amount DESC;
