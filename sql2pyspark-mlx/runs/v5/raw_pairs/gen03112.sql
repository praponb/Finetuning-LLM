WITH base AS (
    SELECT *, COALESCE(supplier_name, 'Unknown') AS supplier_name_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name LIKE 'S%'
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    supplier_name_filled
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS LAST, id) = 1
