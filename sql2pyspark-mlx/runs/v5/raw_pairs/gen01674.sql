SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    o.customer_id,
    o.amount,
    c.country,
    c.city
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
RIGHT JOIN customers c ON o.customer_id = c.id
WHERE oi.order_id IN (15, 23, 24, 32) AND c.age < 36;
