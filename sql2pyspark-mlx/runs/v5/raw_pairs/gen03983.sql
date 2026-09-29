SELECT
    CASE WHEN cost >= 550 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    SUM(stock_qty) AS total_stock_qty,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    BIT_OR(stock_qty) AS stock_qty_bit_or
FROM products
GROUP BY 1
ORDER BY stock_qty_bit_or;
