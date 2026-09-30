SELECT
    category,
    CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values,
    MAX(launch_date) AS latest_launch_date
FROM products
WHERE launch_date > '2025-06-25'
GROUP BY category, CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END;
