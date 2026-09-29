SELECT p.price, t.avg_quantity
FROM products p
LEFT JOIN (SELECT product_id, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY price DESC, avg_quantity DESC;
