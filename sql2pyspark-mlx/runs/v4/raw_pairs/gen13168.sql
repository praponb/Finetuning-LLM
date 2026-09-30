SELECT balance, customer_id
FROM accounts
WHERE YEAR(opened_date) = 2024
ORDER BY balance DESC, customer_id DESC;
