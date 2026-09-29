SELECT account_type, status, balance, LOWER(account_type) AS account_type_lower FROM accounts ORDER BY balance, account_type DESC;
