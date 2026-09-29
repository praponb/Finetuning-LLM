SELECT * FROM (SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, status, balance FROM accounts WHERE balance <= 66000)
PIVOT (MAX(balance) FOR status IN ('OPEN', 'FROZEN', 'CLOSED'));
