SELECT *
FROM (
    SELECT brand, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM products
    WHERE MONTH(launch_date) IN (1, 7, 10)
    GROUP BY brand
) AS s
WHERE stddev_stock_qty < 27
ORDER BY brand, stddev_stock_qty DESC
LIMIT 1
