WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft NULLS LAST) AS sqft_dense_rank,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM stores
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        sqft_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft_share DESC) AS sqft_share_percent_rank
    FROM ranked
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM with_windows
    INTERSECT ALL
    SELECT city, id
    FROM stores
    WHERE region IN ('South', 'East', 'West')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    city,
    CASE WHEN cumulative_sqft >= 76690 THEN 'HIGH' ELSE 'LOW' END AS cumulative_sqft_tier,
    PERCENTILE_APPROX(sqft_share_percent_rank, 0.9) AS sqft_share_percent_rank_p90,
    COUNT_IF(city = 'New York') AS new_york_events,
    PERCENTILE_APPROX(cumulative_sqft, 0.9) AS cumulative_sqft_p90,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    PERCENTILE_APPROX(cumulative_sqft, 0.95) AS cumulative_sqft_p95,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY CUBE (city, CASE WHEN cumulative_sqft >= 76690 THEN 'HIGH' ELSE 'LOW' END)
HAVING new_york_events < 1000
