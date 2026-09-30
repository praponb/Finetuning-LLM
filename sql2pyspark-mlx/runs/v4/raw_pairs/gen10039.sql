SELECT s.end_date, c.signup_date, c.city
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE LOWER(c.name) LIKE '%ran%'
ORDER BY s.end_date;
