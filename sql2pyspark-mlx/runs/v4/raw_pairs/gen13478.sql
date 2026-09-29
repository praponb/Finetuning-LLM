SELECT
    city,
    city_group,
    avg_sqft,
    region_list
FROM (
    SELECT
        city,
        CASE WHEN city IN ('London', 'Tokyo') THEN 'core' ELSE 'other' END AS city_group,
        ROUND(AVG(sqft), 2) AS avg_sqft,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list
    FROM stores
    GROUP BY city, CASE WHEN city IN ('London', 'Tokyo') THEN 'core' ELSE 'other' END
) AS x
WHERE avg_sqft >= 20;
