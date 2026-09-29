SELECT p.id, p.price, t.avg_quantity
FROM products p
INNER JOIN (SELECT product_id, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE unit_price > 140
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_quantity >= 3.3
