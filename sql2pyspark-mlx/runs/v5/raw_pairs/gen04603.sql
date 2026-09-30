SELECT account_type, status
FROM accounts
INTERSECT
SELECT account_type, status
FROM accounts
WHERE account_type = 'CREDIT';
