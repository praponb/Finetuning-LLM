WITH cleaned AS (
    SELECT *, UPPER(country) country_upper
    FROM suppliers
    WHERE city = 'Bangkok'
),
base AS (
    SELECT *, CASE WHEN country = 'JP' THEN 1 ELSE 0 END is_jp
    FROM cleaned
    WHERE city IS DISTINCT FROM 'Tokyo'
),
valid AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_upper,
        is_jp,
        rating / NULLIF(is_jp, 0) rating_per_is_jp,
        COALESCE(city, 'UNKNOWN') resolved_city
    FROM base
    WHERE city IS NOT NULL
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY is_jp, id) = 1
)
SELECT
    id,
    supplier_name,
    country,
    is_jp,
    rating_per_is_jp,
    resolved_city
FROM ranked
ORDER BY rating_per_is_jp NULLS LAST
