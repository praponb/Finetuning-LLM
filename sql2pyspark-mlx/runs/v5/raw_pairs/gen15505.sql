SELECT
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(quantity) AS total_quantity,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE MONTH(last_restock) IN (4, 6, 11)
GROUP BY 1
HAVING stddev_quantity >= 42
ORDER BY avg_quantity DESC;
