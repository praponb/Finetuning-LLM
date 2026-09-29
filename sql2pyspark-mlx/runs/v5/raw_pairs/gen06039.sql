WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(city, 1, 3) AS city_prefix
    FROM suppliers
    WHERE supplier_name LIKE '%Lio%'
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    country,
    rating,
    city_prefix
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS row_num
    FROM cleaned
) ranked
WHERE row_num = 1;
