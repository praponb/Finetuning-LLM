SELECT QUARTER(opened_date) AS opened_quarter, AVG(balance) AS avg_balance, COUNT(balance) AS balance_count, MAX(opened_date) AS latest_opened_date FROM accounts GROUP BY QUARTER(opened_date);
