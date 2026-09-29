WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date NULLS LAST, id) = 1
)
SELECT
    region,
    COUNT_IF(city = 'New York') AS new_york_events,
    MAX(sqft) FILTER (WHERE region = 'Central') AS central_max_sqft,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM ranked
GROUP BY 1
ORDER BY region DESC NULLS FIRST;
