SELECT DATE_TRUNC('MONTH', opened_date) opened_month, CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, ROUND((100 * SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_south
FROM stores
WHERE store_name IS NOT NULL AND city IN ('Singapore', 'Chiang Mai')
GROUP BY 1, 2
ORDER BY pct_south DESC, opened_month, opened_cohort
LIMIT 1;
