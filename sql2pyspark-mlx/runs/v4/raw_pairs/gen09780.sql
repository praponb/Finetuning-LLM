SELECT
    CASE WHEN ss.units >= 34 THEN 'high' WHEN ss.units >= 19 THEN 'mid' ELSE 'low' END AS units_bucket,
    CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    ROUND(AVG(i.quantity), 2) AS avg_quantity,
    MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY CASE WHEN ss.units >= 34 THEN 'high' WHEN ss.units >= 19 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY avg_quantity;
