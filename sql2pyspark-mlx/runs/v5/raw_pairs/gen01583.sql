SELECT amount
FROM transactions
WHERE account_id IN (25, 31, 36)
UNION ALL
SELECT cost
FROM shipments
WHERE order_id IS NOT NULL
ORDER BY amount;
