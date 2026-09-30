SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, c.country, SUM(o.discount) AS total_discount
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, c.country
ORDER BY total_discount DESC;
