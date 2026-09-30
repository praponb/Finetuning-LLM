WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        ROW_NUMBER() OVER w_recent AS seq_no,
        RANK() OVER w_hist AS sqft_rank,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date DESC, id) AS event_seq
    FROM stores
    WINDOW w_recent AS (PARTITION BY city ORDER BY sqft DESC, id), w_hist AS (PARTITION BY city, region ORDER BY sqft DESC)
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    region,
    COUNT_IF(city = 'New York') AS new_york_events,
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT region) AS distinct_regions_2,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(city = 'New York') DESC) AS new_york_events_dense_rank
FROM enriched
GROUP BY CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END, region
ORDER BY seq_no_tier DESC NULLS LAST;
