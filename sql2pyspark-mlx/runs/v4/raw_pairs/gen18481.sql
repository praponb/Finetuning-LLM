SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    category,
    brand,
    SUM(CASE WHEN category = 'Fashion' THEN price ELSE 0 END) AS fashion_price,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    CORR(price, stock_qty) AS corr_price_stock_qty,
    SIZE(COLLECT_SET(category)) AS num_categories
FROM products
GROUP BY DATE_TRUNC('WEEK', launch_date), category, brand
ORDER BY fashion_price NULLS LAST;
