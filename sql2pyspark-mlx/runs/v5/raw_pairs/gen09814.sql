SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, SORT_ARRAY(COLLECT_SET(s.carrier)) AS carrier_list, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.city = 'Tokyo'
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY signup_cohort
LIMIT 5;
