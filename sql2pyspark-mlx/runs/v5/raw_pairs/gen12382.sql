WITH metrics AS (
    SELECT
        *,
        SUM(cost) OVER w_grp AS running_sum_cost,
        MAX(cost) OVER w_grp AS running_max_cost,
        AVG(cost) OVER w_grp AS running_avg_cost,
        COUNT(cost) OVER w_grp AS running_count_cost,
        FIRST_VALUE(cost) IGNORE NULLS OVER w_ordered AS first_known_cost
    FROM products
    WHERE supplier_id IN (14, 30, 40)
    WINDOW w_grp AS (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY brand, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date < s.launch_date
        )
),
filtered AS (
    SELECT
        *,
        running_sum_cost + running_max_cost AS running_sum_cost_plus_running_max_cost,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM not_flagged
    WHERE supplier_id IS NOT NULL
),
metrics_products AS (
    SELECT
        *,
        SUM(price) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        SUM(running_max_cost) OVER (PARTITION BY brand ORDER BY first_known_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_max_cost,
        COUNT(running_sum_cost) OVER (PARTITION BY brand) AS brand_count_running_sum_cost,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) AS event_seq,
        CORR(stock_qty, cost) OVER (PARTITION BY supplier_id) AS corr_stock_qty_cost
    FROM filtered
)
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    supplier_id,
    COUNT(*) FILTER (WHERE brand = 'Acme') AS acme_rows,
    COUNT(*) AS cnt,
    SUM(running_max_cost) FILTER (WHERE category = 'Sports') AS sports_total_running_max_cost,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
    AVG(running_avg_cost) FILTER (WHERE category = 'Home') AS home_avg_running_avg_cost
FROM metrics_products
GROUP BY CUBE (DATE_TRUNC('MONTH', launch_date), supplier_id);
