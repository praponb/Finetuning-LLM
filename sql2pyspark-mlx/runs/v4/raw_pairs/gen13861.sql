SELECT
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort,
    city,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) region_values,
    ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_chiang_mai
FROM stores
GROUP BY 1, 2
