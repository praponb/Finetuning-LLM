SELECT c.country, MONTH(c.signup_date) AS signup_month, COUNT(s.monthly_fee) AS monthly_fee_count
FROM subscriptions s
RIGHT OUTER JOIN customers c ON c.id = s.customer_id
GROUP BY c.country, MONTH(c.signup_date)
HAVING monthly_fee_count >= 3;
