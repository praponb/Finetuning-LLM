SELECT
    city,
    CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END rating_bucket,
    country,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values
FROM suppliers
WHERE country = 'US'
GROUP BY city, rating_bucket, country
ORDER BY country DESC;
