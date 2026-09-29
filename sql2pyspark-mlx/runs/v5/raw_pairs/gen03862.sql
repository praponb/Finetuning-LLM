SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, ROUND((100 * SUM(CASE WHEN o.status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
ORDER BY pct_shipped, signup_month
LIMIT 10;
