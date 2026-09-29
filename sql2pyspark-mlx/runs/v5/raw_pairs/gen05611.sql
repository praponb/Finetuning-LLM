SELECT
    YEAR(opened_date) AS opened_year,
    city,
    region,
    COUNT(DISTINCT sqft) AS unique_regions,
    COUNT(*) AS num_stores
FROM stores
WHERE store_name LIKE 'A%' AND sqft > 9925
GROUP BY opened_year, city, region
