SELECT * FROM (SELECT status, account_type, balance FROM accounts) PIVOT (COUNT(balance) FOR account_type IN ('CREDIT', 'SAVINGS')) ORDER BY status
