WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(units, revenue) AS least_units_revenue,
        CASE WHEN units >= 36 THEN 'High' WHEN units >= 8 THEN 'Medium' ELSE 'Low' END AS units_tier
    FROM store_sales
    WHERE sale_date >= DATE '2023-02-17'
      AND units >= 22
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        units,
        revenue,
        units_tier,
        FLOOR(units) AS units_whole
    FROM valid
    WHERE sale_date >= DATE '2023-05-09'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(units) OVER (PARTITION BY store_id ORDER BY units_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
        CUME_DIST() OVER (ORDER BY units_whole) AS units_whole_cume_dist,
        AVG(units_whole) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_whole_avg_2
    FROM base
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units_whole_cume_dist * 1.07, 2) AS units_whole_cume_dist_scaled
    FROM scored
    WHERE units_whole BETWEEN 24 AND 35
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        units,
        units_tier,
        units_whole_cume_dist,
        units_whole_avg_2,
        units_whole_cume_dist_scaled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_whole_avg_2 DESC, id) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN units_whole_avg_2 >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_whole_avg_2_tier,
    CASE WHEN units >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(units_whole_avg_2), 2) AS avg_units_whole_avg_2,
    APPROX_PERCENTILE(units_whole_avg_2, ARRAY(0.25, 0.5, 0.75)) AS units_whole_avg_2_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2,
    BIT_OR(units) AS units_bit_or
FROM latest
GROUP BY CASE WHEN units_whole_avg_2 >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN units >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(DISTINCT id) > 1
ORDER BY units_whole_avg_2_tier NULLS LAST
