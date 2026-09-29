SELECT * FROM (SELECT status, account_type, balance FROM accounts)
PIVOT (AVG(balance) FOR account_type IN ('CREDIT', 'SAVINGS', 'CHECKING', 'LOAN'))
ORDER BY status
