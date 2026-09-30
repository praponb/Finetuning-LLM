SELECT a.account_type, a.balance, t.merchant, t.txn_time FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.amount > 1400 AND t.is_flagged;
