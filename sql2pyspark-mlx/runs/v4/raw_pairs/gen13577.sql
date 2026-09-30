WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(city) city_lower,
        LOWER(country) country_lower,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 6
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST, id) AS rank_in_group
FROM base
QUALIFY rank_in_group = 1;
