WITH kept_keys AS (
    SELECT city, region
    FROM stores
    INTERSECT
    SELECT city, region
    FROM stores
    WHERE region = 'West'
),
kept_rows AS (
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.city = k.city AND s.region = k.region
)
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SIZE(COLLECT_SET(city)) AS num_cities_2,
    COUNT_IF(city = 'Singapore') AS singapore_events,
    MIN(sqft) AS min_sqft,
    MAX(sqft) AS max_sqft,
    SUM(sqft) FILTER (WHERE city = 'Bangkok') AS bangkok_total_sqft
FROM kept_rows
GROUP BY DATE_TRUNC('WEEK', opened_date)
