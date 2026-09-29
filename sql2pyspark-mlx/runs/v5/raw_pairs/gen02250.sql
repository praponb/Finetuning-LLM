SELECT id, account_type, opened_date, NTILE(3) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS quartile FROM accounts ORDER BY account_type, id DESC;
