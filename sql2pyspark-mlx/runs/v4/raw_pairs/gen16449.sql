WITH valid AS (
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
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
        LEAST(cost, stock_qty) AS least_cost_stock_qty
    FROM products
    WHERE launch_date > DATE '2023-11-28'
),
enriched AS (
    SELECT
        *,
        LAG(days_since_launch_date, 2) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS prev_days_since_launch_date,
        NTILE(10) OVER (PARTITION BY supplier_id ORDER BY price, id) AS price_bucket,
        FIRST_VALUE(least_cost_stock_qty) IGNORE NULLS OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_cost_stock_qty,
        AVG(cost) OVER (PARTITION BY category) AS category_avg_cost,
        SUM(days_since_launch_date) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_launch_date
    FROM valid
),
base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        days_since_launch_date,
        least_cost_stock_qty,
        prev_days_since_launch_date,
        price_bucket,
        first_known_least_cost_stock_qty,
        category_avg_cost,
        cumulative_days_since_launch_date,
        first_known_least_cost_stock_qty - price AS first_known_least_cost_stock_qty_minus_price,
        ROUND(first_known_least_cost_stock_qty * 1.07, 2) AS adjusted_first_known_least_cost_stock_qty,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM enriched
    WHERE launch_date > DATE '2023-06-06'
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY brand ORDER BY prev_days_since_launch_date DESC, id) AS prev_days_since_launch_date_bucket,
        LAG(first_known_least_cost_stock_qty_minus_price, 2) OVER (PARTITION BY supplier_id, category ORDER BY cost, id) AS prev_first_known_least_cost_stock_qty_minus_price,
        AVG(first_known_least_cost_stock_qty) OVER (PARTITION BY category, brand) AS category_avg_first_known_least_cost_stock_qty
    FROM base
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost NULLS FIRST, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
with_windows AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        days_since_launch_date,
        least_cost_stock_qty,
        prev_days_since_launch_date,
        price_bucket,
        first_known_least_cost_stock_qty,
        category_avg_cost,
        cumulative_days_since_launch_date,
        first_known_least_cost_stock_qty_minus_price,
        adjusted_first_known_least_cost_stock_qty,
        brand_clean,
        prev_days_since_launch_date_bucket,
        prev_first_known_least_cost_stock_qty_minus_price,
        category_avg_first_known_least_cost_stock_qty,
        dedup_rank,
        SUM(stock_qty) OVER w_hist AS running_stock_qty,
        AVG(days_since_launch_date) OVER w_recent AS days_since_launch_date_avg_5_prior,
        NTILE(10) OVER w_grp AS first_known_least_cost_stock_qty_bucket,
        AVG(first_known_least_cost_stock_qty) OVER (PARTITION BY supplier_id ORDER BY category_avg_first_known_least_cost_stock_qty, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS first_known_least_cost_stock_qty_avg_5,
        CORR(stock_qty, cost) OVER (PARTITION BY supplier_id) AS corr_stock_qty_cost
    FROM ranked
    WINDOW w_hist AS (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY prev_first_known_least_cost_stock_qty_minus_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY supplier_id ORDER BY first_known_least_cost_stock_qty DESC, id)
)
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    SUM(CASE WHEN category = 'Electronics' THEN first_known_least_cost_stock_qty_minus_price ELSE 0 END) AS electronics_first_known_least_cost_stock_qty_minus_price,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    COUNT(*) FILTER (WHERE category = 'Grocery') AS grocery_rows
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', launch_date)
