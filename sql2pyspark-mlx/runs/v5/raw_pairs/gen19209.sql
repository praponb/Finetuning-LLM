SELECT
    p.category,
    p.brand,
    COUNT(*) AS num_rows,
    SUM(e.quantity) AS total_quantity
FROM order_items e
JOIN products p ON e.product_id = p.id
GROUP BY p.category, p.brand;
