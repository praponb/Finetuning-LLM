WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        tags,
        supplier_id,
        COALESCE(cost, 0) AS cost_filled,
        CONCAT(category, ', ', brand) AS category_brand,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-09-30'
      AND brand IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        tags,
        AVG(stock_qty) OVER w_running AS stock_qty_avg_5_prior,
        LAG(price, 2) OVER w_hist AS prev_price,
        CUME_DIST() OVER (PARTITION BY category ORDER BY cost_filled) AS cost_filled_cume_dist,
        LAST_VALUE(stock_qty, TRUE) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        LAG(cost_filled, 1) OVER w_hist AS prev_cost_filled
    FROM cleaned
    WINDOW w_running AS (PARTITION BY category ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_hist AS (PARTITION BY supplier_id ORDER BY price, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IS NOT NULL
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(last_known_stock_qty) IGNORE NULLS OVER (PARTITION BY category ORDER BY stock_qty_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_stock_qty,
        LAST_VALUE(cost_filled_cume_dist, TRUE) OVER (PARTITION BY category ORDER BY prev_cost_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_filled_cume_dist
    FROM eligible
)
-- Multi-level aggregation
SELECT
    CASE WHEN stock_qty_avg_5_prior >= 323 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_avg_5_prior_tier,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    APPROX_PERCENTILE(prev_cost_filled, ARRAY(0.25, 0.5, 0.75)) AS prev_cost_filled_quartiles,
    MIN(cost_filled_cume_dist) AS min_cost_filled_cume_dist
FROM enriched
GROUP BY 1;
