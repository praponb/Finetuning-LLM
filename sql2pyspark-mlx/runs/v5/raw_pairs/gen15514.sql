SELECT * FROM (SELECT status, account_type, balance FROM accounts WHERE opened_date > '2023-05-17') PIVOT (MAX(balance) FOR account_type IN ('SAVINGS', 'CREDIT', 'LOAN', 'CHECKING')) ORDER BY status;
