WITH remaining_keys AS (
    SELECT city, id
    FROM suppliers
    INTERSECT
    SELECT city, id
    FROM suppliers
    WHERE rating >= 2.9
),
verified AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
valid AS (
    SELECT *, LOWER(supplier_name) AS supplier_name_lower
    FROM verified
    WHERE city IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty = 269
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty = 388
        )
)
SELECT
    id,
    supplier_name,
    country,
    city
FROM eligible
ORDER BY country
