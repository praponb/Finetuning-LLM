WITH matched_keys AS (
    -- Set difference on keys
    SELECT category, supplier_id
    FROM products
    EXCEPT ALL
    SELECT category, supplier_id
    FROM products
    WHERE supplier_id = 29
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date <= s.launch_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM kept
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE stock_qty > 329
),
reconciled_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        category,
        brand,
        cost,
        stock_qty,
        supplier_id,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share,
        COUNT(price) OVER (PARTITION BY brand) AS brand_count_price,
        MIN(cost) OVER (PARTITION BY brand) AS brand_min_cost
    FROM reconciled_products
    WHERE cost > 500
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.brand,
        s.cost,
        s.stock_qty,
        s.supplier_id,
        s.cost_share,
        s.brand_min_cost,
        d.city,
        d.rating,
        d.supplier_name
    FROM metrics s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
)
-- Join a dimension and aggregate
SELECT
    s.country,
    a.brand,
    COUNT(*) AS num_rows,
    AVG(a.cost_share) AS avg_cost_share,
    AVG(a.stock_qty) AS avg_stock_qty
FROM joined a
INNER JOIN suppliers s ON a.supplier_id = s.id
GROUP BY s.country, a.brand
