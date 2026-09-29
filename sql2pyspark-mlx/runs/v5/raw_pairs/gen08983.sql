SELECT status
FROM accounts
WHERE balance > 74000
INTERSECT
SELECT status
FROM payments
