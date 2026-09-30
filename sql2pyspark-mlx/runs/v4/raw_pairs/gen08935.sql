WITH product_totals AS (
    SELECT product_id, COUNT(unit_price) unit_price_count, COUNT(DISTINCT unit_price) unique_products
    FROM order_items
    WHERE quantity IS NOT NULL
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    p.price,
    pt.unit_price_count,
    pt.unique_products
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id
ORDER BY unique_products
