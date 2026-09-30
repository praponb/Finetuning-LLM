SELECT id, account_type, status, balance, CONCAT(account_type, ' - ', status) AS account_type_status FROM accounts
