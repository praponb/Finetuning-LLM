SELECT
    category,
    QUARTER(launch_date) launch_quarter,
    MAX(stock_qty) - MIN(stock_qty) stock_qty_range,
    COUNT(DISTINCT price) unique_brands
FROM products
GROUP BY category, launch_quarter
ORDER BY stock_qty_range, category, launch_quarter
LIMIT 3;
