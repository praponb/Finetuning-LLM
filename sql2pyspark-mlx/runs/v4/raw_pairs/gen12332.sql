SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, QUARTER(c.signup_date) AS signup_quarter, SORT_ARRAY(COLLECT_SET(o.status)) AS status_list
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN stores s ON s.id = o.store_id
GROUP BY DATE_TRUNC('MONTH', c.signup_date), QUARTER(c.signup_date)
ORDER BY signup_quarter DESC, signup_month
LIMIT 5;
