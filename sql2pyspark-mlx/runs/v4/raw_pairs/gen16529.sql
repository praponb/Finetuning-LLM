SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MAX(balance) AS max_balance
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
HAVING MIN(balance) < 38000;
