SELECT *
FROM accounts
WHERE account_type IN ('LOAN', 'SAVINGS') AND account_type = 'SAVINGS' AND balance < 70000
ORDER BY account_type
