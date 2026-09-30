SELECT id, status FROM accounts WHERE account_type IN ('SAVINGS', 'CREDIT', 'CHECKING') UNION ALL SELECT id, status FROM accounts WHERE balance BETWEEN 38000 AND 61000
