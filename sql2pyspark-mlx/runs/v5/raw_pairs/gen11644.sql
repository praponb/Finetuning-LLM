SELECT
    products.id,
    products.cost,
    t.min_last_restock,
    t.max_quantity
FROM products
LEFT JOIN (SELECT product_id, MIN(last_restock) AS min_last_restock, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY product_id) t ON products.id = t.product_id;
