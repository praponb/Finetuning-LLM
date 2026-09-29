SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END last_restock_cohort, SUM(quantity) total_quantity, ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM inventory
WHERE store_id IN (3, 5, 6)
GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY total_quantity DESC;
