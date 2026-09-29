WITH with_windows AS (
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
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS price_bucket,
        NTILE(4) OVER w_running AS cost_bucket,
        CUME_DIST() OVER w_part AS stock_qty_cume_dist,
        NTILE(10) OVER (PARTITION BY category ORDER BY price, id) AS price_bucket_2,
        AVG(cost) OVER (PARTITION BY category, supplier_id ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM products
    WINDOW w_running AS (PARTITION BY category ORDER BY cost, id), w_part AS (PARTITION BY brand ORDER BY stock_qty DESC NULLS LAST)
)
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    COUNT(*) AS event_count,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev
FROM with_windows
GROUP BY DATE_TRUNC('QUARTER', launch_date), DATE_TRUNC('YEAR', launch_date)
ORDER BY year_start;
