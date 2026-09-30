WITH matched_keys AS (
    -- Set difference on keys
    SELECT region, city
    FROM stores
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE opened_date >= '2023-06-06'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    JOIN matched_keys k ON s.region = k.region AND s.city = k.city
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN sqft >= 13331 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(*) AS event_count,
    MAX(sqft) FILTER (WHERE city = 'Bangkok') AS bangkok_max_sqft,
    SUM(sqft) FILTER (WHERE city = 'Berlin') AS berlin_total_sqft,
    APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90
FROM verified
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', opened_date), CASE WHEN sqft >= 13331 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', opened_date)), ())
ORDER BY event_count DESC;
