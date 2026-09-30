SELECT
    products.id,
    products.supplier_id,
    products.category,
    t.unit_price_count
FROM products
INNER JOIN (SELECT product_id, COUNT(unit_price) AS unit_price_count
    FROM order_items
    GROUP BY product_id) t ON products.id = t.product_id;
