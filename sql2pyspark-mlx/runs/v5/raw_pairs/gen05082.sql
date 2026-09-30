SELECT id, customer_id, DATE_ADD(opened_date, 60) AS opened_date_plus_60d
FROM accounts
ORDER BY id ASC;
