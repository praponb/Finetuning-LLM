SELECT *
FROM (
    SELECT
        CASE WHEN units >= 41 THEN 'high' WHEN units >= 18 THEN 'mid' ELSE 'low' END units_bucket,
        CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END sale_cohort,
        MIN(revenue) min_revenue,
        ROUND(STDDEV(revenue), 2) stddev_revenue
    FROM store_sales
    GROUP BY CASE WHEN units >= 41 THEN 'high' WHEN units >= 18 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
) agg
WHERE min_revenue > 2600
ORDER BY min_revenue;
