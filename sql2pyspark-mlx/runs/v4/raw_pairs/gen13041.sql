WITH matched_keys AS (
    -- Set intersection on keys
    SELECT brand, id
    FROM products
    INTERSECT
    SELECT brand, id
    FROM products
    WHERE cost BETWEEN 640 AND 710
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
)
SELECT
    product_name,
    brand,
    price,
    stock_qty,
    tags
FROM reconciled
ORDER BY price;
