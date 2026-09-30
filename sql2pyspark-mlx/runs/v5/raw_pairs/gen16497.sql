SELECT accounts.id, accounts.status
FROM accounts
WHERE accounts.id IN (SELECT account_id FROM transactions WHERE merchant IN ('Grab', 'Shell'))
ORDER BY id DESC
