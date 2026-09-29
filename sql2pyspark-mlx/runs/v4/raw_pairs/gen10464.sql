SELECT
    MONTH(launch_date) launch_month,
    CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort,
    CASE WHEN price >= 990 THEN 'high' WHEN price >= 440 THEN 'mid' ELSE 'low' END price_bucket,
    MIN(price) min_price,
    SORT_ARRAY(COLLECT_SET(brand)) brand_list
FROM products
WHERE launch_date BETWEEN '2023-03-02' AND '2025-05-25' AND category <> 'Electronics' AND YEAR(launch_date) = 2024
GROUP BY 1, 2, 3
ORDER BY 4 DESC, launch_month DESC
