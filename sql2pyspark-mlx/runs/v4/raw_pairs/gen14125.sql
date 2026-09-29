SELECT id, opened_date, balance, account_type FROM accounts WHERE status <> 'FROZEN' OR status IN ('FROZEN', 'CLOSED', 'OPEN') ORDER BY opened_date, balance;
