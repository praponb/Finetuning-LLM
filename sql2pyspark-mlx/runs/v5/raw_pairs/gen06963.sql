SELECT status
FROM accounts
EXCEPT
SELECT status
FROM payments
