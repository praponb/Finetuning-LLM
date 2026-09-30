WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        opened_date,
        SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft,
        LAST_VALUE(sqft) IGNORE NULLS OVER w_part AS last_known_sqft
    FROM stores
    WINDOW w_part AS (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    region,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT_IF(region = 'East') AS east_events,
    PERCENTILE_APPROX(region_sum_sqft, ARRAY(0.25, 0.5, 0.75)) AS region_sum_sqft_quartiles
FROM scored
GROUP BY 1
HAVING COUNT(DISTINCT id) >= 1
ORDER BY distinct_ids ASC NULLS FIRST;
