SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, MAX(quantity) AS max_quantity
FROM inventory
WHERE last_restock < '2023-06-01' AND product_id IN (4, 6, 7, 9) AND store_id IN (3, 4, 5)
GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING COUNT(quantity) >= 2
