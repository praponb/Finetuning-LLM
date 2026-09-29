SELECT
    p.id,
    p.supplier_id,
    p.brand,
    t.unique_stores
FROM products p
LEFT JOIN (SELECT product_id, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    WHERE quantity <= 211
    GROUP BY product_id) t ON p.id = t.product_id
