WITH with_windows AS (
    SELECT
        *,
        AVG(stock_qty) OVER (ORDER BY stock_qty ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_2_prior,
        CORR(stock_qty, cost) OVER (PARTITION BY brand) AS corr_stock_qty_cost,
        NTILE(10) OVER (PARTITION BY brand ORDER BY price ASC, id ASC) AS price_bucket
    FROM products
)
SELECT
    CASE WHEN price_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_bucket_tier,
    category,
    supplier_id,
    APPROX_PERCENTILE(price_bucket, ARRAY(0.25, 0.5, 0.75)) AS price_bucket_quartiles,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    APPROX_PERCENTILE(price_bucket, ARRAY(0.25, 0.5, 0.75)) AS price_bucket_quartiles_2,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    ROUND(AVG(price_bucket), 2) AS avg_price_bucket
FROM with_windows
GROUP BY CASE WHEN price_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, category, supplier_id
