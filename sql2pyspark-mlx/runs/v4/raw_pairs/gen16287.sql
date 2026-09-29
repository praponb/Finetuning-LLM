SELECT c.is_active, QUARTER(c.signup_date) AS signup_quarter, COUNT(DISTINCT s.customer_id) AS unique_customers
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE TO_DATE(s.started_at) < '2024-02-01'
GROUP BY c.is_active, QUARTER(c.signup_date)
HAVING COUNT(DISTINCT s.customer_id) > 5;
