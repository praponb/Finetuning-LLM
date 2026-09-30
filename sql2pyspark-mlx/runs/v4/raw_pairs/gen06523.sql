SELECT
    o.id,
    o.store_id,
    o.amount,
    t.max_unit_price,
    t.revenue
FROM orders o
JOIN (SELECT order_id, MAX(unit_price) AS max_unit_price, SUM(unit_price * quantity) AS revenue
    FROM order_items
    WHERE quantity BETWEEN 4 AND 5
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.revenue >= 42
ORDER BY o.id;
