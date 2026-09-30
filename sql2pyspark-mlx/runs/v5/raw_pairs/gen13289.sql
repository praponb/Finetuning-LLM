WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM stores
    WHERE opened_date > DATE '2023-08-03'
      AND sqft <> 6240
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM valid
    EXCEPT DISTINCT
    SELECT city, id
    FROM stores
    WHERE opened_date > '2024-05-28'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.product_id IN (1, 6, 8)
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 21
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(region)) AS num_regions,
    MAX(sqft) AS max_sqft,
    COUNT_IF(region = 'North') AS north_events,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles
FROM eligible
GROUP BY DATE_TRUNC('YEAR', opened_date)
HAVING MAX(sqft) < 1000000
ORDER BY num_regions DESC;
