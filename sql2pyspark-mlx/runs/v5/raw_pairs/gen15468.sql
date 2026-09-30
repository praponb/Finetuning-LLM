SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    city,
    COUNT(sqft) AS sqft_count,
    COUNT(DISTINCT sqft) AS unique_regions
FROM stores
GROUP BY DATE_TRUNC('MONTH', opened_date), city;
