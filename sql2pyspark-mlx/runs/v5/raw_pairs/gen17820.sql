WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost BETWEEN 490 AND 580
        )
)
SELECT
    id,
    supplier_name,
    country,
    city
FROM kept;
