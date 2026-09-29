SELECT QUARTER(c.signup_date) AS signup_quarter, COUNT(*) AS num_orders, MAX(o.amount) AS max_amount
FROM orders o
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE s.city IN ('Singapore', 'New York')
GROUP BY QUARTER(c.signup_date)
ORDER BY max_amount;
