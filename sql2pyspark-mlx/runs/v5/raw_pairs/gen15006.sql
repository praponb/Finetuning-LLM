SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price, MAX(oi.unit_price) AS max_unit_price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE p.cost >= 400
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY launch_cohort
