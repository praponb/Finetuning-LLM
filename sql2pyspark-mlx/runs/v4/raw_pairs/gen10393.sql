SELECT
    *,
    ROUND(cost / SUM(cost) OVER (PARTITION BY category, brand), 4) AS cost_share,
    FIRST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty
FROM products;
