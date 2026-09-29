SELECT
    CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    brand,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count
FROM products
GROUP BY 1, 2
HAVING acme_count > 27;
