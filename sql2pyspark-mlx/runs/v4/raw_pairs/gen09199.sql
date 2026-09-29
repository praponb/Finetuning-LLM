SELECT * FROM (SELECT account_type, status, balance FROM accounts) PIVOT (MAX(balance) FOR status IN ('CLOSED', 'FROZEN', 'OPEN')) ORDER BY account_type
