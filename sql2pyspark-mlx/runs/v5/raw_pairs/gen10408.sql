SELECT * FROM (SELECT status, account_type, balance FROM accounts) PIVOT (MAX(balance) FOR account_type IN ('CREDIT', 'LOAN')) ORDER BY status
