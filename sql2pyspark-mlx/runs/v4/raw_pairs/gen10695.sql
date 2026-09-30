SELECT transactions.id, transactions.merchant, transactions.is_flagged FROM transactions WHERE transactions.account_id IN (SELECT id FROM accounts WHERE account_type NOT IN ('CREDIT', 'LOAN'))
