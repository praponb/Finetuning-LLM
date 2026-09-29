SELECT SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance, ROUND(AVG(balance), 2) AS avg_balance, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM accounts
WHERE balance >= 71000
ORDER BY savings_balance DESC
