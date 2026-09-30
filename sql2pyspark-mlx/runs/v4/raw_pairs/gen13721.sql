SELECT
    store_id,
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
GROUP BY 1, 2
ORDER BY total_quantity, store_id, last_restock_cohort
LIMIT 10
