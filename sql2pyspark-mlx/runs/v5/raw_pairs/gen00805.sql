SELECT
    id,
    brand,
    stock_qty,
    launch_date
FROM (
    SELECT
        id,
        brand,
        stock_qty,
        launch_date,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS rnk
    FROM products
) AS ranked
WHERE rnk = 1
ORDER BY brand;
