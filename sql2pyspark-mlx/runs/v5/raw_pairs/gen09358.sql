WITH product_totals AS (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    WHERE order_id IN (12, 21, 25)
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    p.brand,
    pt.unique_products,
    pt.quantity_range
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id;
