SELECT
    city,
    opened_month,
    region_values,
    sqft_count
FROM (
    SELECT
        city,
        MONTH(opened_date) AS opened_month,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values,
        COUNT(sqft) AS sqft_count
    FROM stores
    GROUP BY city, MONTH(opened_date)
) AS s
WHERE sqft_count >= 9
ORDER BY sqft_count
