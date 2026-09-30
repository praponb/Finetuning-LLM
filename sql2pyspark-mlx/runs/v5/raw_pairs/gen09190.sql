SELECT id, opened_date, balance, DAYOFWEEK(opened_date) AS opened_date_dayofweek FROM accounts WHERE balance >= 56000 AND account_type = 'LOAN'
