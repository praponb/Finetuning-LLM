WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(city) AS clean_city,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE city = 'Berlin'
),
matched_keys AS (
    -- Set difference on keys
    SELECT region, id
    FROM base
    EXCEPT DISTINCT
    SELECT region, id
    FROM stores
    WHERE opened_date >= '2025-10-29'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.region = k.region AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (1, 3, 4)
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        resolved_region,
        CEIL(sqft) AS sqft_whole
    FROM not_flagged
    WHERE region IS NOT NULL
      AND opened_date BETWEEN '2023-04-01' AND '2024-04-17'
),
filtered_stores AS (
    -- Basic cleansing and defaults
    SELECT *, sqft - sqft_whole AS sqft_minus_sqft_whole
    FROM filtered
    WHERE opened_date >= DATE '2023-03-24'
      AND region IS NOT DISTINCT FROM 'South'
)
-- Grouped metrics
SELECT
    region,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN city = 'Tokyo' THEN sqft_minus_sqft_whole ELSE 0 END) AS tokyo_sqft_minus_sqft_whole,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count
FROM filtered_stores
GROUP BY region
HAVING bangkok_count < 1000
