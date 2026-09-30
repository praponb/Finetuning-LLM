WITH matched_keys AS (
    SELECT supplier_id, brand
    FROM products
    EXCEPT DISTINCT
    SELECT supplier_id, brand
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
),
kept_rows AS (
    SELECT s.*
    FROM products s
    JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.brand = k.brand
)
SELECT
    id,
    category,
    brand,
    price,
    cost,
    launch_date,
    CASE WHEN category RLIKE '^(Books|Fashion)$' THEN 'EXTENDED' WHEN category RLIKE '^(Electronics|Home)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS category_zone
FROM kept_rows
ORDER BY id, cost DESC;
