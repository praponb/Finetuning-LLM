SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    brand,
    CORR(stock_qty, price) AS corr_stock_qty_price,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count,
    STDDEV_SAMP(price) AS price_stddev,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count,
    STDDEV_SAMP(cost) AS cost_stddev
FROM products
GROUP BY quarter_start, brand
ORDER BY brand DESC NULLS FIRST
