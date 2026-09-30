SELECT
    region,
    opened_month,
    city_list,
    total_sqft
FROM (
    SELECT
        region,
        MONTH(opened_date) AS opened_month,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        SUM(sqft) AS total_sqft
    FROM stores
    GROUP BY region, MONTH(opened_date)
) AS sub
WHERE total_sqft < 70000
ORDER BY total_sqft DESC, region, opened_month
LIMIT 50;
