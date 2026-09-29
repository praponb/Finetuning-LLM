SELECT status, COUNT(DISTINCT balance) AS unique_customers
FROM accounts
WHERE YEAR(opened_date) >= 2024 AND status != 'CLOSED'
GROUP BY status
ORDER BY 1, unique_customers DESC;
