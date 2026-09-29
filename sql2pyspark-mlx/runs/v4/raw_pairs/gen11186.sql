SELECT *
FROM accounts
WHERE account_type IN ('SAVINGS', 'CHECKING', 'CREDIT') AND account_type = 'CHECKING'
ORDER BY account_type, id
LIMIT 5;
