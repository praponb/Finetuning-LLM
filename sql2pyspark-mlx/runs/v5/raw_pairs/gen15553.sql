WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        rating,
        CONCAT(city, ' ', supplier_name) AS city_supplier_name
    FROM suppliers
    WHERE city IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        city_supplier_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(country) AS country_lower
    FROM ranked
    WHERE country IN ('DE', 'SG', 'TH')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 27
        )
),
summary AS (
    -- Multi-level aggregation
    SELECT
        country,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count_2
    FROM kept
    GROUP BY country
)
SELECT
    country,
    country_list,
    CASE WHEN country RLIKE '^(UK|JP)$' THEN 'CORE' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS country_zone
FROM summary;
