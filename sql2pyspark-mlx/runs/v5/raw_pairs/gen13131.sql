SELECT
    p.price,
    p.cost,
    t.revenue,
    t.avg_quantity
FROM products p
JOIN (SELECT product_id, SUM(quantity * unit_price) AS revenue, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE order_id = 40
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.revenue > 43
ORDER BY price DESC, 3 DESC
