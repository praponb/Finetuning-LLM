SELECT supplier_id, total_cost, stddev_stock_qty
FROM (
    SELECT supplier_id, SUM(cost) AS total_cost, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM products
    WHERE LENGTH(product_name) > 10
    GROUP BY supplier_id
) AS t
WHERE stddev_stock_qty < 75
ORDER BY stddev_stock_qty DESC
