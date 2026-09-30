SELECT QUARTER(c.signup_date) signup_quarter, ROUND(AVG(s.monthly_fee), 2) avg_monthly_fee
FROM subscriptions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE c.country <> 'JP'
GROUP BY QUARTER(c.signup_date)
ORDER BY avg_monthly_fee DESC
