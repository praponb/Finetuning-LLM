SELECT MONTH(c.signup_date) AS signup_month, c.segment, MIN(s.started_at) AS first_started_at
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.city = 'London'
GROUP BY MONTH(c.signup_date), c.segment;
