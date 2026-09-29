SELECT status, 'order' AS source
FROM orders
UNION
SELECT status, 'account' AS source
FROM accounts
WHERE YEAR(opened_date) >= 2025
ORDER BY source DESC;
