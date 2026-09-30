SELECT * FROM (SELECT account_type, status, balance FROM accounts)
PIVOT (COUNT(balance) FOR status IN ('FROZEN', 'OPEN', 'CLOSED'))
ORDER BY account_type;
