WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key
    FROM suppliers
    WHERE city IS NOT NULL
      AND country = 'JP'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Acme', 'Stark', 'Initech')
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, country
    FROM not_flagged
    EXCEPT ALL
    SELECT city, country
    FROM suppliers
    WHERE rating > 2.2
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN matched_keys k ON s.city = k.city AND s.country = k.country
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_supplier_name_key,
        CEIL(rating) AS rating_whole
    FROM kept_rows
    WHERE city IS NOT NULL
      AND supplier_name LIKE '%Supply%'
)
-- Grouped metrics
SELECT
    CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    city,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_per_singapore,
    SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) AS de_rating,
    AVG(rating) AS avg_rating,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    GROUPING(CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END, city), (CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY num_cities DESC NULLS FIRST;
