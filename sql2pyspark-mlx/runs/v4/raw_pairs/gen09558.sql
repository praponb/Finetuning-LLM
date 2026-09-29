SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY last_restock_cohort
HAVING MAX(quantity) - MIN(quantity) >= 57
