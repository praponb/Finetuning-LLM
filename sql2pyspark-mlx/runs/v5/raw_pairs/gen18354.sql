SELECT account_type, COUNT(DISTINCT balance) AS unique_customers
FROM accounts
GROUP BY account_type
ORDER BY 2 DESC;
