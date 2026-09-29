SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE opened_date > '2025-02-01') PIVOT (SUM(balance) FOR account_type IN ('CREDIT', 'CHECKING')) ORDER BY status;
