SELECT account_type, DATE_TRUNC('MONTH', opened_date) AS opened_month, MIN(opened_date) AS min_opened_date FROM accounts WHERE status NOT IN ('CLOSED', 'FROZEN') GROUP BY 1, 2;
