SELECT id, account_type, balance, RANK() OVER (PARTITION BY account_type ORDER BY balance) AS rnk FROM accounts WHERE status IS NOT NULL;
