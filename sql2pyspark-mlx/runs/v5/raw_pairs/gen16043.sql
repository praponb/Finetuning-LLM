WITH matched_keys AS (
    -- Set difference on keys
    SELECT region, id
    FROM stores
    EXCEPT DISTINCT
    SELECT region, id
    FROM stores
    WHERE city IN ('Chiang Mai', 'Bangkok', 'London')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    INNER JOIN matched_keys k ON s.region = k.region AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN sqft >= 13512 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN region = 'South' THEN sqft ELSE 0 END) AS south_sqft,
    APPROX_PERCENTILE(sqft, 0.95) AS sqft_p95,
    COUNT(DISTINCT city) AS distinct_cities,
    MIN(sqft) AS min_sqft
FROM verified
GROUP BY DATE_TRUNC('QUARTER', opened_date), CASE WHEN sqft >= 13512 THEN 'LARGE' ELSE 'SMALL' END
HAVING STDDEV_SAMP(sqft) > 0
ORDER BY south_sqft DESC
