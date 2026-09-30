WITH filtered AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        UPPER(country) AS country_upper
    FROM suppliers
    WHERE country <> 'SG'
),
valid AS (
    SELECT *, REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug
    FROM filtered
    WHERE city IN ('Tokyo', 'Bangkok')
),
filtered_suppliers AS (
    SELECT
        id,
        supplier_name,
        city,
        CONCAT(city, ' - ', country) AS city_country,
        COALESCE(city, 'N/A') AS resolved_city
    FROM valid
    WHERE supplier_name LIKE 'T%'
),
base AS (
    SELECT
        id,
        supplier_name,
        city,
        resolved_city,
        SUBSTRING(supplier_name, 1, 2) AS supplier_name_prefix,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'OTHER') AS city_filled
    FROM filtered_suppliers
    WHERE UPPER(supplier_name) LIKE '%IAM%'
)
SELECT
    id,
    city,
    resolved_city,
    supplier_name_prefix,
    supplier_name_lower,
    city_filled,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS city_zone
FROM base
ORDER BY city DESC NULLS LAST
