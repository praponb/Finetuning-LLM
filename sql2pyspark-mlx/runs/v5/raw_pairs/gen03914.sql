SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') signup_month, c.segment, MIN(s.started_at) min_started_at
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.city IS NULL
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), c.segment;
