SELECT id, customer_id, CONCAT(account_type, ' ', status) account_type_status
FROM accounts
WHERE opened_date >= '2023-06-20' AND YEAR(opened_date) >= 2023;
