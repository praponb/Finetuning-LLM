WITH ProductStats AS (
    SELECT product_id, COUNT(quantity) AS quantity_count, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE store_id IN (2, 3, 5)
    GROUP BY product_id
)
SELECT
    products.supplier_id,
    products.launch_date,
    ProductStats.quantity_count,
    ProductStats.unique_products
FROM products
INNER JOIN ProductStats ON products.id = ProductStats.product_id
