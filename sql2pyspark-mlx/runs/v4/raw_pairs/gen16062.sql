SELECT account_type, SUM(balance) AS total_balance FROM accounts WHERE YEAR(opened_date) >= 2023 OR status NOT IN ('FROZEN', 'CLOSED', 'OPEN') GROUP BY account_type;
