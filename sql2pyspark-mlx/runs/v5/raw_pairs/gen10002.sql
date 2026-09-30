WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LOWER(city) AS city_lower,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE country = 'TH'
),
filtered_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        city_lower,
        LENGTH(country) AS country_length
    FROM filtered
    WHERE city IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(country_length) AS country_length_whole
    FROM filtered_suppliers
    WHERE country_length > 31
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND SIZE(c.tags) >= 1
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost IS NOT NULL
        )
)
SELECT
    id,
    city,
    country_length,
    country_length_whole,
    CASE WHEN city RLIKE '^(Singapore|Chiang Mai)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS city_zone
FROM kept
ORDER BY city NULLS LAST
