SELECT DATE_FORMAT(customers.signup_date, 'yyyy-MM') AS signup_month, MIN(sessions.started_at) AS first_started_at
FROM sessions
INNER JOIN customers ON customers.id = sessions.customer_id
GROUP BY DATE_FORMAT(customers.signup_date, 'yyyy-MM')
ORDER BY signup_month ASC
