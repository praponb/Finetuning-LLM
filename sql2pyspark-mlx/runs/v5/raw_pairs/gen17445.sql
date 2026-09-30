WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (5, 15, 32, 33)
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        launch_date,
        tags,
        supplier_id,
        AVG(stock_qty) OVER w_grp AS last3_avg_stock_qty,
        MIN(stock_qty) OVER w_grp AS last3_min_stock_qty,
        MAX(stock_qty) OVER w_grp AS last3_max_stock_qty
    FROM kept
    WHERE stock_qty < 362
    WINDOW w_grp AS (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.supplier_name,
        p.rating,
        p.country
    FROM metrics src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, id
    FROM enriched
    INTERSECT
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id = 25
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
-- Window metrics over several different partitions
SELECT
    id,
    category,
    launch_date,
    tags,
    supplier_id,
    last3_avg_stock_qty,
    last3_max_stock_qty,
    supplier_name,
    country,
    COUNT(last3_max_stock_qty) OVER (PARTITION BY category, supplier_id) AS category_count_last3_max_stock_qty
FROM reconciled
