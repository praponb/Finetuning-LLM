WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        price / NULLIF(stock_qty, 0) AS price_per_stock_qty,
        CONCAT_WS('-', product_name, brand) AS product_name_brand_key
    FROM products
    WHERE supplier_id IS NOT NULL
      AND launch_date < '2023-08-01'
),
kept_keys AS (
    SELECT category, supplier_id
    FROM filtered
    INTERSECT
    SELECT category, supplier_id
    FROM products
    WHERE supplier_id IS NULL
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
),
metrics AS (
    SELECT
        *,
        AVG(price) OVER (PARTITION BY supplier_id) AS supplier_id_avg_price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY price_per_stock_qty NULLS LAST) AS price_per_stock_qty_dense_rank,
        AVG(price_per_stock_qty) OVER (PARTITION BY brand) AS brand_avg_price_per_stock_qty
    FROM kept_rows
),
cleaned AS (
    SELECT
        *,
        cost - supplier_id_avg_price AS cost_minus_supplier_id_avg_price,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM metrics
    WHERE supplier_id IS NOT NULL
)
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    brand,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    GROUPING(DATE_TRUNC('QUARTER', launch_date)) AS grouping_level
FROM cleaned
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', launch_date), brand), (DATE_TRUNC('QUARTER', launch_date)))
ORDER BY home_count DESC;
