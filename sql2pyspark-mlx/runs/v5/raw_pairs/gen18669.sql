WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(supplier_name) AS supplier_name_upper,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE city = 'Tokyo'
)
SELECT city
FROM filtered
INTERSECT ALL
SELECT city
FROM suppliers
WHERE city IN ('Chiang Mai', 'Singapore')
