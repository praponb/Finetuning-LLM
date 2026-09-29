SELECT
    CASE WHEN ss.revenue >= 4200 THEN 'high' WHEN ss.revenue >= 1100 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    SUM(i.quantity) AS total_quantity,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY CASE WHEN ss.revenue >= 4200 THEN 'high' WHEN ss.revenue >= 1100 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
