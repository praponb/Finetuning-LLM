WITH scored AS (
    -- Window metrics over several different partitions
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
        NTILE(4) OVER w_recent AS stock_qty_bucket,
        FIRST_VALUE(price, TRUE) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price
    FROM products
    WINDOW w_recent AS (ORDER BY stock_qty DESC, id)
),
enriched AS (
    -- Attach suppliers attributes
    SELECT a.*, s.city
    FROM scored a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT category, supplier_id
    FROM enriched
    EXCEPT DISTINCT
    SELECT category, supplier_id
    FROM products
    WHERE brand IN ('Acme', 'Initech', 'Stark')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(stock_qty_bucket) OVER (PARTITION BY brand) AS brand_avg_stock_qty_bucket,
        AVG(price) OVER (PARTITION BY city) AS city_avg_price,
        COUNT(stock_qty_bucket) OVER (PARTITION BY city, supplier_id) AS city_count_stock_qty_bucket
    FROM kept_rows
)
SELECT
    s.category,
    s.brand,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    s.stock_qty_bucket,
    s.first_known_price,
    s.brand_avg_stock_qty_bucket,
    (SELECT AVG(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS avg_product_cost
FROM with_windows s
ORDER BY avg_product_cost DESC
