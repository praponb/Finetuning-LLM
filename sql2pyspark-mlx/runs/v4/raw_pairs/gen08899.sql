SELECT
    region,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS sqft_per_chiang_mai,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
GROUP BY region
HAVING ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) >= 0;
