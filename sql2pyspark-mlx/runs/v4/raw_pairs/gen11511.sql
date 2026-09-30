WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS FIRST, id) AS rn
        FROM stores
    ) t
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    CASE WHEN sqft >= 16131 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT_IF(region = 'South') AS south_events,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(DISTINCT city) AS distinct_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM deduped
GROUP BY DATE_TRUNC('WEEK', opened_date), CASE WHEN sqft >= 16131 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_sqft < 0
ORDER BY week_start
