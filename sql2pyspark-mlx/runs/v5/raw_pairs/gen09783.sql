WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(stock_qty) AS stock_qty_whole
    FROM products
    WHERE YEAR(launch_date) = 2025
      AND supplier_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM filtered
    EXCEPT ALL
    SELECT supplier_id
    FROM products
    WHERE cost < 480
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.supplier_id = k.supplier_id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date >= s.launch_date
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT supplier_id, id
    FROM eligible
    EXCEPT ALL
    SELECT supplier_id, id
    FROM products
    WHERE brand = 'Acme'
),
verified_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        category,
        brand,
        cost
    FROM verified_products
) src
PIVOT (
    AVG(cost) AS total,
    COUNT(1) AS cnt
    FOR brand IN ('Globex', 'Acme', 'Stark')
)
