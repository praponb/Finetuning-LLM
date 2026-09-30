SELECT status, balance, account_type, DATE_TRUNC('MONTH', opened_date) AS opened_date_period FROM accounts;
