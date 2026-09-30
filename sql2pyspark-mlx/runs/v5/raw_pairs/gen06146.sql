SELECT c.is_active, MAX(s.start_date) AS last_start_date
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
ORDER BY 2 DESC;
