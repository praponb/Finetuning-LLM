WITH cleaned AS (
    SELECT
        id,
        city,
        rating,
        TRIM(city) clean_city
    FROM suppliers
    WHERE city IN ('New York', 'Tokyo')
)
SELECT
    city,
    COUNT(*) AS cnt,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    STDDEV_SAMP(rating) AS rating_stddev,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS row_count_2
FROM cleaned
GROUP BY 1
ORDER BY city DESC
