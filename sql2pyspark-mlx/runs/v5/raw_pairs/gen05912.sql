WITH matched_keys AS (
    -- Set intersection on keys
    SELECT brand, supplier_id
    FROM products
    INTERSECT ALL
    SELECT brand, supplier_id
    FROM products
    WHERE brand = 'Globex'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    supplier_id,
    CASE WHEN product_name RLIKE '^(Office Chair|Desk Lamp)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS product_name_zone
FROM kept_rows
ORDER BY brand DESC NULLS LAST
