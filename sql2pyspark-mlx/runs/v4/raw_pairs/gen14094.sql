WITH ranked AS (
    SELECT
        id,
        brand,
        stock_qty,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS row_num
    FROM products
)
SELECT
    id,
    brand,
    stock_qty,
    launch_date
FROM ranked
WHERE row_num <= 2
ORDER BY brand
