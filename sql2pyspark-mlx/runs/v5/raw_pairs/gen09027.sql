SELECT *, LENGTH(account_type) AS account_type_length
FROM accounts
WHERE balance < 75000 OR status IN ('CLOSED', 'OPEN', 'FROZEN');
