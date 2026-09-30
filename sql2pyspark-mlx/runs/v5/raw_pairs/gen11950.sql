SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
WHERE NOT (region IN ('Central', 'West', 'East') AND sqft <= 15429 AND store_name LIKE '%Mall%')
GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
