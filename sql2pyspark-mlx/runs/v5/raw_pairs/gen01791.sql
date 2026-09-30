WITH base AS (
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CONCAT(category, ' - ', product_name) AS category_product_name,
        stock_qty + cost AS stock_qty_plus_cost
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        stock_qty_plus_cost
    FROM base
) src
PIVOT (
    AVG(stock_qty_plus_cost) AS amount
    FOR brand IN ('Initech' AS initech, 'Acme' AS acme)
)
