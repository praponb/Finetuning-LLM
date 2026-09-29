SELECT status
FROM accounts
WHERE opened_date > '2025-04-01'
UNION
SELECT status
FROM payments
WHERE amount BETWEEN 430 AND 1190
