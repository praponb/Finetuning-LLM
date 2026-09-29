SELECT a.opened_date, a.balance, t.account_id, t.txn_type, t.is_flagged FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.account_type IN ('SAVINGS', 'CHECKING', 'CREDIT')
