WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND MONTH(x.order_date) IN (4, 7, 8)
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS FIRST) AS row_num
        FROM eligible
    ) t
    WHERE row_num = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT region, city
    FROM ranked
    INTERSECT ALL
    SELECT region, city
    FROM stores
    WHERE opened_date BETWEEN '2024-04-18' AND '2024-10-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.region = k.region AND s.city = k.city
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(row_num / sqft, 2) row_num_per_sqft
    FROM kept_rows
    WHERE region IS NOT NULL
      AND region IS NOT DISTINCT FROM 'South'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) sqft_share,
        STDDEV_SAMP(sqft) OVER (PARTITION BY city) city_stddev_samp_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) sqft_share_2
    FROM filtered
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT_IF(region = 'Central') AS central_events,
    APPROX_PERCENTILE(row_num_per_sqft, ARRAY(0.25, 0.5, 0.75)) AS row_num_per_sqft_quartiles,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows
FROM enriched
GROUP BY 1
ORDER BY central_events DESC NULLS LAST
