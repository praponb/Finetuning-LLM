SELECT account_type, DATE_SUB(opened_date, 57) AS opened_date_minus_57d FROM accounts WHERE balance >= 32000;
