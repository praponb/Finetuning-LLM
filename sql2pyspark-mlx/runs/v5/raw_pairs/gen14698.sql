SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, SUM(i.quantity) AS total_quantity, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
FULL OUTER JOIN products p ON p.id = i.product_id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING total_quantity > 700
