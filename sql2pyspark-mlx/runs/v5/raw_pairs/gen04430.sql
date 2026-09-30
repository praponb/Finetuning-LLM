SELECT status
FROM accounts
UNION ALL
SELECT status
FROM shipments
WHERE shipped_date <= '2023-05-01';
