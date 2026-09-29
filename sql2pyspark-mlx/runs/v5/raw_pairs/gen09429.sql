SELECT * FROM (SELECT account_type, status, balance FROM accounts) PIVOT (AVG(balance) FOR status IN ('CLOSED', 'FROZEN'));
