WITH numbered AS (
    SELECT
        id,
        category,
        launch_date,
        stock_qty,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY launch_date) pos
    FROM products
)
SELECT *
FROM numbered
WHERE pos <= 2;
