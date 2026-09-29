WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(country, ', ', city) AS country_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name LIKE '%Sia%'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date BETWEEN '2024-06-26' AND '2025-06-01'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_city,
        PERCENT_RANK() OVER (ORDER BY rating DESC) AS rating_percent_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM not_flagged
    WHERE supplier_name LIKE 'S%'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        rating_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_share DESC, id) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS rating_share_tier,
    country,
    COUNT(DISTINCT country) AS distinct_countries,
    SUM(rating_share) FILTER (WHERE country = 'JP') AS jp_total_rating_share,
    APPROX_PERCENTILE(rating_share, ARRAY(0.25, 0.5, 0.75)) AS rating_share_quartiles,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    APPROX_PERCENTILE(rating_share, 0.9) AS rating_share_p90,
    GROUPING(CASE WHEN rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END, country), (CASE WHEN rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END))
HAVING distinct_countries < 1000
ORDER BY distinct_countries NULLS FIRST
