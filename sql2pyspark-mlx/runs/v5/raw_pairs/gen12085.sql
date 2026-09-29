WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(price, cost) OVER (PARTITION BY brand) AS corr_price_cost,
        AVG(price) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_avg_2
    FROM products
    WHERE LENGTH(product_name) >= 7
)
-- Multi-level aggregation
SELECT
    CASE WHEN price_avg_2 >= 979 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_avg_2_tier,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    COUNT(DISTINCT supplier_id) AS distinct_supplier_ids,
    STDDEV_SAMP(cost) AS cost_stddev,
    SUM(CASE WHEN category = 'Grocery' THEN price ELSE 0 END) AS grocery_price,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM with_windows
GROUP BY CASE WHEN price_avg_2 >= 979 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', launch_date)
ORDER BY price_avg_2_tier
