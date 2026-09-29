SELECT c.segment, ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee, MAX(s.end_date) AS latest_end_date
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.segment
ORDER BY latest_end_date
