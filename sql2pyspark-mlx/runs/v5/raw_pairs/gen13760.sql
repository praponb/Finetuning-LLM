SELECT *
FROM accounts
WHERE NOT (account_type <> 'LOAN' OR status = 'CLOSED');
