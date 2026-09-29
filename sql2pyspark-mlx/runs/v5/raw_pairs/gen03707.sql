SELECT MONTH(c.signup_date) AS signup_month, SUM(CASE WHEN s.device = 'tablet' THEN 1 ELSE 0 END) AS tablet_count
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY MONTH(c.signup_date)
ORDER BY signup_month DESC
