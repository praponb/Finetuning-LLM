WITH latest AS (
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) = 1
)
SELECT
    id,
    category,
    brand,
    cost,
    stock_qty,
    launch_date,
    tags
FROM latest
ORDER BY stock_qty
