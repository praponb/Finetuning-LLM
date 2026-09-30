WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', brand, product_name) AS brand_product_name_key,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM products
    WHERE supplier_id IN (4, 22, 26, 29)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(product_name)) AS product_name_normalized,
        CASE WHEN category IN ('Electronics', 'Fashion') THEN 'Tier 1' ELSE 'Other' END AS category_group
    FROM valid
    WHERE launch_date > DATE '2023-04-09'
      AND category NOT IN ('Books', 'Sports', 'Fashion')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id = 38
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        brand,
        cost,
        stock_qty,
        launch_date,
        brand_product_name_key,
        resolved_brand,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_dense_rank,
        AVG(stock_qty) OVER w_part AS stock_qty_avg_3_prior
    FROM eligible
    WINDOW w_part AS (PARTITION BY supplier_id ORDER BY cost ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN stock_qty >= 389 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
    MAX(stock_qty) AS max_stock_qty,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    MAX(cost) FILTER (WHERE brand = 'Stark') AS stark_max_cost,
    COUNT(*) FILTER (WHERE brand = 'Initech') AS initech_rows
FROM with_windows
GROUP BY 1
ORDER BY stock_qty_bit_or DESC;
