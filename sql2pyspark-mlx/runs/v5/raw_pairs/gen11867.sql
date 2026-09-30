SELECT id, balance, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label FROM accounts WHERE opened_date > '2024-03-01'
