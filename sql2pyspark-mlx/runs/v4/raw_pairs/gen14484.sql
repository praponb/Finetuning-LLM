SELECT
    p.launch_date,
    i.product_id,
    i.quantity,
    i.last_restock
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
WHERE p.category = 'Electronics' AND p.cost >= 280;
