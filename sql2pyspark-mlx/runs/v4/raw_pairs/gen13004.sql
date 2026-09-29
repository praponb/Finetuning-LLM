SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SUM(sqft) AS total_sqft,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS sqft_per_south
FROM stores
GROUP BY DATE_TRUNC('YEAR', opened_date)
HAVING SUM(sqft) < 0
ORDER BY sqft_per_south NULLS LAST;
