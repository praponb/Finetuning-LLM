SELECT id, account_type, status
FROM accounts
WHERE status NOT IN ('OPEN', 'FROZEN')
EXCEPT
SELECT id, account_type, status
FROM accounts
WHERE customer_id = 33
ORDER BY status;
