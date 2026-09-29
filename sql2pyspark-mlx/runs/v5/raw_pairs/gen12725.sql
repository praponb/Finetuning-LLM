WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM products
    QUALIFY RANK() OVER (PARTITION BY brand ORDER BY stock_qty) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(stock_qty, price) AS greatest_stock_qty_price,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM deduped
    WHERE brand != 'Umbrella'
),
enriched AS (
    -- Attach suppliers attributes
    SELECT src.*, p.rating
    FROM filtered src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
),
deduped_products AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT brand, category
    FROM deduped_products
    EXCEPT
    SELECT brand, category
    FROM products
    WHERE brand NOT IN ('Globex', 'Stark', 'Umbrella')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped_products s
    INNER JOIN matched_keys k ON s.brand = k.brand AND s.category = k.category
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY category) AS category_count_cost,
        SUM(greatest_stock_qty_price) OVER (PARTITION BY supplier_id) AS supplier_id_sum_greatest_stock_qty_price
    FROM reconciled
    WHERE supplier_id IN (4, 29, 30, 31)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    supplier_id,
    ROUND(AVG(supplier_id_sum_greatest_stock_qty_price), 2) AS avg_supplier_id_sum_greatest_stock_qty_price,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    GROUPING(DATE_TRUNC('YEAR', launch_date)) AS grouping_level
FROM metrics
GROUP BY CUBE (DATE_TRUNC('YEAR', launch_date), supplier_id)
