WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(category) AS category_upper,
        COALESCE(stock_qty, 0) AS stock_qty_filled,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date > TO_DATE('2023-08-08')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity <> 5
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT brand
    FROM not_flagged
    EXCEPT ALL
    SELECT brand
    FROM products
    WHERE YEAR(launch_date) >= 2024
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN matched_keys k ON s.brand = k.brand
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.stock_qty_filled,
        s.resolved_brand,
        b.country,
        b.city,
        b.supplier_name
    FROM reconciled s
    INNER JOIN suppliers b
        ON s.supplier_id = b.id
    WHERE b.rating IS NULL
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        src.category,
        src.price,
        src.cost,
        src.supplier_id,
        src.stock_qty_filled,
        src.country,
        src.city,
        p.rating,
        p.city AS supplier_city,
        p.supplier_name AS supplier_supplier_name
    FROM with_dims src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
)
-- Multi-level aggregation
SELECT
    city,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    AVG(stock_qty_filled) AS avg_stock_qty_filled
FROM with_dims_products
GROUP BY city
ORDER BY avg_stock_qty_filled
