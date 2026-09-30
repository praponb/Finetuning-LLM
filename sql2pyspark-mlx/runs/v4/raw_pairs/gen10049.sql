SELECT accounts.id, accounts.opened_date, accounts.status
FROM accounts
WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.txn_time < '2024-01-01');
