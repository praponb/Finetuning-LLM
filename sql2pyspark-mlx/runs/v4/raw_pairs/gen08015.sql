WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        price + stock_qty price_plus_stock_qty,
        CASE WHEN stock_qty >= 365 THEN 'High' WHEN stock_qty >= 134 THEN 'Medium' ELSE 'Low' END stock_qty_level,
        COALESCE(brand, 'NONE') brand_clean
    FROM products
    WHERE brand IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_clean,
        price * price_plus_stock_qty price_times_price_plus_stock_qty
    FROM valid
    WHERE launch_date > DATE '2024-03-12'
      AND supplier_id IN (10, 14, 16)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(price) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) price_avg_2_prior,
        FIRST_VALUE(price, TRUE) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_price,
        CUME_DIST() OVER (ORDER BY stock_qty DESC) stock_qty_cume_dist
    FROM base
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date <= s.launch_date
        )
),
with_windows_products AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_clean,
        price_times_price_plus_stock_qty,
        price_avg_2_prior,
        first_known_price,
        stock_qty_cume_dist,
        SUM(price) OVER (PARTITION BY category, supplier_id ORDER BY price_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_price,
        CUME_DIST() OVER (ORDER BY price_times_price_plus_stock_qty DESC) price_times_price_plus_stock_qty_cume_dist,
        LAST_VALUE(price_avg_2_prior) IGNORE NULLS OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_price_avg_2_prior
    FROM kept
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_ordered price_avg_2_prior_rank,
        AVG(first_known_price) OVER (PARTITION BY supplier_id, brand ORDER BY cumulative_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) first_known_price_avg_3_prior,
        ROUND(first_known_price / SUM(first_known_price) OVER (PARTITION BY brand), 4) first_known_price_share,
        ROUND(first_known_price / SUM(first_known_price) OVER (PARTITION BY category, brand), 4) first_known_price_share_2
    FROM with_windows_products
    WINDOW w_ordered AS (PARTITION BY category ORDER BY price_avg_2_prior NULLS FIRST)
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.brand_clean,
    s.price_times_price_plus_stock_qty,
    s.price_avg_2_prior,
    (SELECT SUM(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_cost
FROM metrics s
ORDER BY id NULLS LAST;
