SELECT *
FROM (
    SELECT store_id, CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    WHERE quantity < 121
    GROUP BY store_id, CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
) AS t
WHERE avg_quantity > 68
ORDER BY avg_quantity DESC, store_id;
