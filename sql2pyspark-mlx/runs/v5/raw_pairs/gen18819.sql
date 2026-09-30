WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.product_id = 7
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        LAG(cost) OVER (PARTITION BY category ORDER BY stock_qty, id) prev_cost,
        LAG(price) OVER (PARTITION BY category ORDER BY stock_qty, id) prev_price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) row_seq,
        AVG(cost) OVER (ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) cost_avg_3_prior
    FROM eligible
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_hist cost_bucket,
        AVG(prev_cost) OVER w_part prev_cost_avg_2
    FROM metrics
    WINDOW w_hist AS (ORDER BY cost, id), w_part AS (PARTITION BY category ORDER BY price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.sale_date,
        latest.store_sale_revenue
    FROM scored s
    JOIN LATERAL (
            SELECT c.sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cost_avg_3_prior >= 701 THEN 'HIGH' ELSE 'LOW' END AS cost_avg_3_prior_tier,
        category,
        SUM(stock_qty) AS total_stock_qty,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        GROUPING(CASE WHEN cost_avg_3_prior >= 701 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM latest_match
    GROUP BY GROUPING SETS ((CASE WHEN cost_avg_3_prior >= 701 THEN 'HIGH' ELSE 'LOW' END, category), (CASE WHEN cost_avg_3_prior >= 701 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    cost_avg_3_prior_tier,
    category,
    total_stock_qty,
    category_list
FROM stats;
