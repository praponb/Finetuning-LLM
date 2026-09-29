SELECT id, account_type, balance, YEAR(opened_date) AS opened_date_year FROM accounts WHERE opened_date <= '2023-08-20' ORDER BY opened_date_year, id LIMIT 20;
