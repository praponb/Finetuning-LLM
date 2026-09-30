SELECT
    p.id,
    p.supplier_id,
    p.brand,
    t.avg_quantity,
    t.unit_price_count
FROM products p
LEFT JOIN (SELECT product_id, AVG(quantity) AS avg_quantity, COUNT(unit_price) AS unit_price_count
    FROM order_items
    WHERE unit_price IS NOT NULL
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_quantity >= 4.3;
