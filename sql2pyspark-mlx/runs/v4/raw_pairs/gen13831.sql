WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        sqft,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-11-29'
      AND region IS NOT NULL
      AND city = 'Bangkok'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) AS sqft_dense_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM latest
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < '2025-02-01'
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', opened_date_plus_months) AS activity_month,
    city,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(*) AS event_count,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS sqft_per_singapore
FROM not_flagged
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', opened_date_plus_months), city), (DATE_TRUNC('MONTH', opened_date_plus_months)), ())
ORDER BY num_cities DESC;
