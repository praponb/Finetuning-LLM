WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        INITCAP(TRIM(brand)) AS brand_title
    FROM products
    WHERE launch_date >= TO_DATE('2023-02-27')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        supplier_id,
        brand_title,
        AVG(price) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_avg_2,
        MIN(cost) OVER (PARTITION BY supplier_id) AS supplier_id_min_cost,
        NTILE(3) OVER (ORDER BY price DESC, id ASC) AS price_bucket
    FROM base
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, supplier_id
    FROM enriched
    INTERSECT ALL
    SELECT id, supplier_id
    FROM products
    WHERE LOWER(TRIM(product_name)) = 'running shoes'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.units > 21
        )
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY price_avg_2 DESC) AS price_avg_2_cume_dist,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) AS cost_share,
        CUME_DIST() OVER w_part AS price_cume_dist,
        MAX(stock_qty) OVER (PARTITION BY supplier_id) AS supplier_id_max_stock_qty,
        CUME_DIST() OVER w_running AS stock_qty_cume_dist
    FROM not_flagged
    WHERE price_avg_2 = 779
    WINDOW w_part AS (PARTITION BY supplier_id ORDER BY price ASC NULLS FIRST), w_running AS (ORDER BY stock_qty ASC NULLS FIRST)
)
-- Aggregate with conditional metrics
SELECT
    supplier_id,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    ROUND(AVG(price), 2) AS avg_price,
    APPROX_PERCENTILE(price_avg_2, ARRAY(0.25, 0.5, 0.75)) AS price_avg_2_quartiles,
    APPROX_PERCENTILE(cost_share, 0.5) AS cost_share_p50,
    ROUND(AVG(stock_qty_cume_dist), 2) AS avg_stock_qty_cume_dist
FROM enriched_products
GROUP BY supplier_id;
