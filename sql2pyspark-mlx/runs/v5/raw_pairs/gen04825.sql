SELECT * FROM (SELECT account_type, status, balance FROM accounts WHERE balance >= 69000) PIVOT (MAX(balance) FOR status IN ('CLOSED', 'FROZEN', 'OPEN'));
