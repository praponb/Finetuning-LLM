SELECT transactions.amount, transactions.txn_time, transactions.merchant, accounts.customer_id, accounts.balance FROM transactions LEFT JOIN accounts ON transactions.account_id = accounts.id
