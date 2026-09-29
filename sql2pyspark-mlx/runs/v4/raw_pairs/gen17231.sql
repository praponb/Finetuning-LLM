SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    city,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count
FROM stores
GROUP BY opened_month, city;
