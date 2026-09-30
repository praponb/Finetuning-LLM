WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE rating > 2.8
),
matched_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM base
    INTERSECT
    SELECT city, id
    FROM suppliers
    WHERE country IN ('JP', 'TH', 'UK')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        supplier_name,
        country,
        city,
        supplier_name_slug,
        city_filled,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'NONE') AS resolved_city
    FROM kept_rows
    WHERE city IS NOT NULL
),
base_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        supplier_name,
        city,
        supplier_name_slug,
        city_filled,
        supplier_name_lower,
        resolved_city,
        LENGTH(city) AS city_length
    FROM filtered
    WHERE country IN ('US', 'TH', 'JP')
)
-- Aggregate with conditional metrics
SELECT
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MIN(city_length) AS min_city_length,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM base_suppliers
GROUP BY city
HAVING COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) >= 1
ORDER BY city DESC NULLS LAST;
