SELECT id, account_type, balance, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS row_num FROM accounts
