WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE city = 'Berlin' AND opened_date BETWEEN '2023-04-01' AND '2024-05-25'
),
stores_stats AS (
    SELECT
        city,
        DATE_FORMAT(opened_date, 'yyyy-MM') opened_month,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values,
        COUNT(DISTINCT sqft) unique_regions
    FROM filtered_stores
    GROUP BY city, DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT
    city,
    opened_month,
    city_values,
    unique_regions
FROM stores_stats
WHERE unique_regions < 8
ORDER BY opened_month;
