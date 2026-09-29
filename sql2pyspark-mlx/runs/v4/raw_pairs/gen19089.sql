SELECT account_type
FROM accounts
WHERE customer_id = 18 AND opened_date >= '2024-06-01'
ORDER BY account_type;
