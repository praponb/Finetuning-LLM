SELECT c.is_active, MAX(s.end_date) AS last_end_date, SUM(CASE WHEN s.status = 'PAUSED' THEN s.monthly_fee ELSE 0 END) AS paused_monthly_fee
FROM subscriptions s
RIGHT JOIN customers c ON c.id = s.customer_id
GROUP BY c.is_active
ORDER BY last_end_date DESC
