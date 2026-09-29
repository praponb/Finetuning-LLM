SELECT id, account_type FROM accounts WHERE opened_date BETWEEN '2023-06-01' AND '2023-10-01' UNION ALL SELECT id, account_type FROM accounts WHERE balance < 65000
