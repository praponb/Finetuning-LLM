SELECT accounts.id, accounts.opened_date, accounts.balance FROM accounts WHERE EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
