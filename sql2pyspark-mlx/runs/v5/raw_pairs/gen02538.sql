SELECT 'product' AS origin, launch_date FROM products UNION ALL SELECT 'account' AS origin, opened_date FROM accounts WHERE account_type IN ('CHECKING', 'CREDIT')
