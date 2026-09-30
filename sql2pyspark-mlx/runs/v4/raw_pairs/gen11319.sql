SELECT
    p.id,
    p.launch_date,
    p.supplier_id,
    t.latest_last_restock,
    t.avg_quantity
FROM products p
LEFT JOIN (SELECT product_id, MAX(last_restock) AS latest_last_restock, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY avg_quantity DESC
