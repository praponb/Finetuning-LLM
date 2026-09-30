WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS LAST, id) = 1
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    CASE WHEN supplier_name RLIKE '^(Lion City Imports|Global Supply)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS supplier_name_zone
FROM deduped
