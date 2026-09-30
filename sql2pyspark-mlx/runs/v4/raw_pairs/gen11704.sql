SELECT status
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
EXCEPT
SELECT status
FROM payments;
