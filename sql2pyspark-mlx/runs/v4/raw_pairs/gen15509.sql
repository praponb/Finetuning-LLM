SELECT YEAR(c.signup_date) AS signup_year, MAX(o.discount) - MIN(o.discount) AS discount_range
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY YEAR(c.signup_date)
