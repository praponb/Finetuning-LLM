SELECT * FROM (SELECT status, account_type, balance FROM accounts)
PIVOT (COUNT(balance) FOR account_type IN ('LOAN', 'CHECKING', 'CREDIT'));
