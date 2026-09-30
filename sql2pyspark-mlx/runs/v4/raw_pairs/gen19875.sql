SELECT
    ss.store_id,
    CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    SUM(i.quantity) AS total_quantity,
    MAX(i.quantity) AS max_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id = 5
GROUP BY ss.store_id, CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY sale_cohort;
