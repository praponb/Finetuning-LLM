SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    SUM(sqft) AS total_sqft,
    COUNT(sqft) AS sqft_count,
    MAX(sqft) AS max_sqft
FROM stores
GROUP BY opened_month
ORDER BY max_sqft DESC;
