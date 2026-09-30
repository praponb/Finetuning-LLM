WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN rating >= 4.4 THEN 'High' WHEN rating >= 2.1 THEN 'Medium' ELSE 'Low' END AS rating_level
    FROM suppliers
    WHERE country IN ('DE', 'US', 'UK')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(country)) AS country_title
    FROM base
    WHERE city IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.category IN ('Electronics', 'Sports', 'Books')
        )
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating_level
FROM not_flagged
ORDER BY id, city DESC
