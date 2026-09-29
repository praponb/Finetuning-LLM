SELECT balance, CONCAT_WS('-', status, account_type) AS status_account_type_key, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
FROM accounts
WHERE status != 'OPEN'
ORDER BY balance
